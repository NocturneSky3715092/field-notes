# Node.js Batch Summarization: Async Lifecycles for Document Collections and Result Export

Short answer: treat multiple-document summarization as a durable job with an explicit input snapshot, per-document state, and an immutable export manifest; let the Node.js API accept and observe that job, while workers handle bounded attempts and idempotent commits.

The API call is the easy part. The production question is what a caller can prove after a timeout, a retry, or a worker restart. I design around that proof first, then choose a queue and model interface that can meet the completion SLO. I've seen teams spend a week tuning concurrency while leaving publication semantics implicit; that optimization only made the ambiguous state arrive faster.

Start with the invariant.

## The failure pattern worth designing around

Consider a normal sequence: a client submits 2,000 documents, loses the response, and retries with the same payload. If the server has no durable request identity, two jobs enter the queue. Both can produce valid summaries, so a green success-rate chart hides the duplicate until someone downloads the export. This is a common distributed-systems shape, not a special property of any one model API.

The invariant should be narrow: a tuple of tenant, document ID, content digest, and summary-policy version names one logical operation. Submission and execution may be repeated; the committed result for that tuple must converge to one record. Exactly-once execution is not a credible promise across HTTP, a queue, a model call, a database, and object storage. A conditional, idempotent commit is.

Publication is a state transition.

The awkward case is worth spelling out because it is where otherwise careful implementations lose their audit trail. Worker A completes the model call and uploads an object, then pauses before recording the item result. Its lease expires. Worker B claims the same item, produces an equivalent or slightly different summary, and reaches the commit first. If the store treats the object path as the identity, A can later overwrite B or leave two plausible files with one document ID. If the store treats the item ledger as the identity, the losing write becomes a harmless duplicate attempt, and reconciliation can identify the one committed version. The export manifest must read from that ledger, not from a directory listing, because a listing cannot tell you which attempt satisfied the policy version or whether an object was superseded. The same reasoning applies to cancellation: a request can mark a job as cancelling, but a worker that already holds a lease still needs a safe boundary before it stops. Make that boundary observable in the item state machine, and make the manifest builder include only terminal, validated outcomes. This is more database work than a single `POST` handler, but it is the work that lets support answer “which documents are in this download?” without guessing from timing.

That distinction changes the resource model. A job stores the caller's idempotency key and input snapshot. An item stores stable document identity, digest, and policy version. Attempts are disposable history with lease and retry metadata. An export is an immutable manifest of successful item versions and their checksums. Progress counters are useful, but `completed_items == total_items` does not prove that every exported object is readable.

There is a boundary. A tiny request that fits inside a strict deadline may be better served synchronously with a small retry budget. A continuously changing corpus needs streaming checkpoints rather than a closed batch. The async pattern is for work whose recovery and publication semantics matter more than shaving one network round trip. The catch is that every durable state adds retention, reconciliation, and on-call work; don't call it simpler just because the client sees one endpoint.

## How should a Node.js API run document batches and publish results?

Admission should validate metadata, freeze the input list, and persist the job before it enqueues work. Return `202 Accepted`, a stable job ID, and a status URL. If the same idempotency key arrives again, return the existing job rather than creating another one. Keep the status response compact: aggregate counts, timestamps, cancellation state, and an export locator when one is published. A client-side backoff policy and an entity validator prevent a fleet of dashboards from turning polling into its own incident.

Preparation is where token capacity gets decided. Count tokens with the tokenizer that matches the selected runtime, record the chunking and overlap policy, and retain those versions with each item so a replay means the same thing later. The `tiktoken` project documents a byte-pair encoding tokenizer for OpenAI models; that is a useful reminder that character counts are not a portable token budget. The tokenizer choice should remain an explicit configuration boundary, not a hidden helper.

Workers claim items with a lease and a deadline. A lease expiry must permit redelivery, so the final write needs a compare-and-set condition on the operation key and content digest. Classify failures before retrying: malformed input becomes a terminal item outcome, while a rate limit or network timeout consumes a bounded attempt with jitter. An ambiguous commit is resolved by reading the operation key before making another model call.

Export comes last. Write each result under an immutable object name, validate required metadata and checksums, then publish a manifest that lists exactly those objects. Store the manifest under a temporary identity and expose its final identity only after the write is complete. Readers get one publication point, even if the underlying object store is eventually consistent.

## A small Go commit path for safe retries

The edge service can be Node.js while the correctness boundary stays language-neutral. This Go sketch shows the part I expect to see in a design review: repeated delivery returns the stored result, and a conditional commit owns the race between two workers.

```go
package batch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type Item struct {
	Tenant, DocumentID, PolicyVersion string
	Content                           []byte
}

type Summary struct {
	Text  string
	Usage int
}

type Store interface {
	Lookup(ctx context.Context, operationKey string) (Summary, bool, error)
	Commit(ctx context.Context, operationKey, contentDigest string, result Summary) (Summary, error)
}

type Summarizer interface {
	Summarize(ctx context.Context, content []byte) (Summary, error)
}

func Process(ctx context.Context, store Store, model Summarizer, item Item) (Summary, error) {
	digest := sha256.Sum256(item.Content)
	contentDigest := hex.EncodeToString(digest[:])
	operationKey := item.Tenant + ":" + item.DocumentID + ":" + contentDigest + ":" + item.PolicyVersion

	if existing, ok, err := store.Lookup(ctx, operationKey); err != nil {
		return Summary{}, err
	} else if ok {
		return existing, nil
	}

	result, err := model.Summarize(ctx, item.Content)
	if err != nil {
		return Summary{}, err
	}
	if result.Text == "" {
		return Summary{}, errors.New("empty summary")
	}

	return store.Commit(ctx, operationKey, contentDigest, result)
}
```

The lookup is an optimization, not the lock. `Commit` must enforce uniqueness in a transaction or equivalent conditional write, and it should return the already committed value when another worker won the race. Keep retry loops outside this function, with a job deadline and a maximum attempt count; unbounded retries turn a dependency incident into a capacity incident.

## Which operating model fits the SLO?

I use this buy-vs-build table in roadmap reviews because “managed” can hide several different transfers of pager duty. Score each option against error budget, staffing, data controls, and the exit plan.

| Approach | Team owns | Useful when | The catch |
|---|---|---|---|
| Build queue and workers | Admission, leases, retries, storage, exports, observability | Scheduling and data controls are specialized | Highest on-call and capacity-planning load |
| Managed async execution | Lifecycle contract, validation, reconciliation | Required retry and retention semantics are provided | Quotas and portability need explicit tests |
| Hybrid orchestration | Internal ledger and manifest; replaceable execution | You want one control plane over changing runtimes | Two operational boundaries complicate ownership |
| Synchronous request plus local queue | Queue, concurrency, retries, result store | Batches are small and deadlines are generous | Your team still implements most async semantics |

My default is the hybrid boundary: keep identity, policy versions, reconciliation, and publication inside the platform, and treat execution as replaceable. That is not a promise of effortless migration. Prompt behavior, tokenization, rate limits, and output quality differ, so portability requires a golden corpus and acceptance ranges.

Choose a managed job facility when its cancellation, retry, retention, and export consistency satisfy the SLO and the reduced pager load is material. Build when regulation or a specialized scheduler makes those controls non-negotiable. This recommendation is not suitable when a team cannot staff the queue, storage, and incident response; stick with a smaller synchronous design when the batch fits the request deadline and the simpler failure surface is worth the lower throughput.

## Capacity and release gates

Capacity planning starts with documents per completion window, then expands into token volume, chunks per item, worker concurrency, lease duration, and export bytes. The mean document size is a poor proxy for the tail. Track token-count and chunk-count percentiles, because a few large inputs can hold leases, amplify retries, and consume the entire batch SLO before the queue looks alarming.

The dashboard should show queue age, oldest runnable item, lease expirations, attempts per item, model-call latency, conditional-commit conflicts, terminal item classes, manifest publication lag, and reconciliation drift. Alert on missed or threatened user deadlines, not queue depth alone. Traces can carry job and item IDs; logs should carry policy versions and redacted operation keys, never raw document text.

Release has two gates. First, replay a fixed evaluation corpus through the new prompt, model, tokenizer, and chunk policy, comparing quality and resource use against declared ranges. Second, canary the scheduler and commit path with forced lease expiry and duplicate delivery; the expected result is one logical outcome per operation key. A reconciliation job should derive expected manifest membership from the item ledger and compare it with published objects.

Before launch, document who owns a stuck job, how long inputs and outputs are retained, how callers resume downloads, and what cancellation guarantees mean. Those are part of the API contract. The exact queue or model can change; the recovery contract cannot.

## References

- https://github.com/openai/tiktoken
- https://elevenlabs.io/docs
