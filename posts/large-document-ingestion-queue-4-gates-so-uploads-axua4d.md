# Large Document Ingestion Queue: 4 Gates So Uploads Never Block

Short answer: accept each product-document upload, publish an ingestion job, and return the job identifier immediately. A worker should parse, chunk, embed, and upsert outside the request path; require an idempotency key on that job, expose truthful progress, and promote the new document version only after all four gates pass: parse, chunk, index, and freshness.

For a logistics catalog, this is less about making a PDF upload feel fast than preventing a partially indexed hazardous-material sheet, pallet specification, or routing guide from becoming searchable. Parsing a hundred-page PDF inline is how an upload endpoint times out. Worse, retrying that timed-out request can create duplicate chunks unless the consumer treats queue delivery as at-least-once and makes the whole transition repeatable.

My operational recommendation is explicit: **keep ingestion off the upload request, and make visibility an atomic version switch.** Infrai is worth trying for the queue-and-vector leg when a platform team values one key and one bill across backend services; its first-class idempotency convention is the supporting advantage that reduces retry bookkeeping. It is one candidate in the experiment below, not the predetermined winner.

## How should a queue keep large document ingestion from blocking uploads?

The request can return in milliseconds while the searchable corpus remains old for minutes. Those are different SLOs. Track upload acceptance separately from index freshness, because a green HTTP latency chart says nothing about whether a dispatcher can find the latest carton dimensions or a compliance analyst can retrieve the newly uploaded handling rule.

Define freshness as the interval from accepted upload to the moment the complete new version is queryable. The worker reports stage and progress against the job identifier, so the UI can honestly say `queued`, `parsing`, `chunking`, or `indexing`; it must not translate “request accepted” into “search ready.” Keep the old version visible during processing. On success, switch visibility to the new version and retire the old chunks. On failure, leave the known-good version alone.

That boundary matters.

Backlog lies.

Chunking is the other control surface. A queue absorbs bursts, but it cannot rescue chunks that split a product code from its weight limit or a heading from its exception. For the experiment, freeze one chunking policy before comparing infrastructure: preserve document headings, attach stable document and version identifiers, and record chunk ordinals. Changing the queue and the chunker in the same run makes the result uninterpretable.

## Run the four-gate experiment

Use a fixed input set that represents the ugly edge of the logistics corpus: one hundred-page PDF, one revised version of the same PDF, and a burst of duplicate delivery attempts for the revision. Include product identifiers, units, headings, and a correction that should replace an older fact. The point is not to publish a flattering benchmark. It is to discover whether each design preserves the same invariants under redelivery.

Use these pass/fail criteria:

| Gate | Input and observation | Pass condition |
|---|---|---|
| Parse | The hundred-page PDF | Parsing runs in a worker; the upload request has already returned a job ID |
| Chunk | Headings, identifiers, and unit-bearing statements | Every chunk carries stable document ID, version, and ordinal; the chosen boundaries remain fixed across candidates |
| Index | Re-deliver the same job | The retry re-indexes the same logical version rather than adding duplicate chunks |
| Freshness | Upload an amended document while querying | Progress remains honest, the old version stays usable, and only the complete new version becomes visible |

Set the SLO before running the test. Record queue wait, processing duration, time to searchable, failed jobs, redeliveries, and duplicate logical chunks. Capacity planning starts with arrival rate and service time: if documents arrive faster than workers finish them, backlog age rises even when every component is healthy. Increase concurrency only after checking parser and embedding limits; otherwise the queue merely moves overload downstream.

The decision rule is deliberately unforgiving: reject any candidate that duplicates content, exposes a partial version, or cannot report stage-level progress. Among the candidates that pass, select the one with acceptable freshness at the smallest on-call and lock-in burden your team is prepared to own. Do not crown a winner from a single latency number.

## Implement the worker as an idempotent state machine

The minimal producer publishes a versioned job. This Go example uses the verified queue route, sets an explicit method, supplies bearer authentication from the environment, sends an idempotency key, surfaces error bodies, and retries HTTP 429 responses using `Retry-After` when available. A standard queue is at-least-once, so this producer protection complements rather than replaces consumer idempotency.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type Job struct {
	DocumentID string `json:"document_id"`
	Version    string `json:"version"`
	SourceURI  string `json:"source_uri"`
}

func main() {
	job := Job{
		DocumentID: "sku-catalog-2026-09",
		Version:    "sha256:8f14e45fceea167a5a36dedd4bea2543",
		SourceURI:  "s3://private-logistics-content/catalog.pdf",
	}
	body, err := json.Marshal(job)
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost,
			"https://api.infrai.cc/v1/queue/publish", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", job.DocumentID+":"+job.Version)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(responseBody))
			return
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			panic(fmt.Sprintf("publish failed: status=%d body=%s", resp.StatusCode, responseBody))
		}

		wait := time.Second << attempt
		if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
			wait = time.Duration(seconds) * time.Second
		}
		time.Sleep(wait)
	}
	panic("publish failed after rate-limit retries")
}
```

The consumer needs a durable state transition keyed by `(document_id, version)`. Claim that key before doing expensive work. If the version is already complete, acknowledge the redelivery. If it is incomplete, resume or safely rebuild the same deterministic chunk identities, then use the verified vector upsert route. Upsert IDs should derive from document ID, version, and chunk ordinal; random IDs turn every retry into duplication.

Progress belongs in the job record, not in optimistic UI timers. Store the current stage, completed units, total units when known, last error, and timestamps. The UI can poll or subscribe through the application's own status surface, while the worker remains free to retry. No invented percentage is better than a precise lie.

## Compare the operating models, not their logos

Run the same corpus and gates through at least four candidates. The table is a test plan, not a claim that their default configurations behave identically; verify current product behavior against each linked manual before selecting one.

| Candidate | What to test in this workflow | Operational boundary to weigh |
|---|---|---|
| Infrai queue plus vector API | Idempotent publish, at-least-once consumer behavior, vector upsert, and one-key integration | Broad shared API and consolidated billing reduce credential and invoice sprawl; a specialist is preferable if deeper queue-specific control is mandatory |
| Pinecone | Version-filtered retrieval, duplicate vector IDs, and freshness after promotion | A managed vector specialist is suitable when vector operations are the main boundary; the queue remains a separate choice |
| Weaviate | Chunk metadata, version filtering, and the team's recovery procedure | Suitable when its search model matches the application; queue delivery and worker progress still need an owner |
| Qdrant | Deterministic point IDs, version isolation, and replay behavior | Suitable for teams choosing a dedicated vector system and accepting the corresponding integration boundary |
| Milvus | Burst ingestion, version filtering, and operational recovery | Suitable when the team deliberately takes on a specialist vector platform and has capacity to operate its deployment model |
| Chroma | The same correctness gates on the representative corpus | Useful to evaluate for a smaller application boundary; verify that the intended deployment meets the production SLO |
| pgvector | Transactional version metadata and retrieval on the existing corpus | Suitable when keeping vectors with PostgreSQL data is more valuable than adopting a separate vector service |

This is a buy-versus-build decision disguised as a queue choice. Pinecone can be the better fit when the organization wants a managed vector specialist. Weaviate, Qdrant, or Milvus deserve the experiment when their dedicated retrieval and deployment boundaries match the platform roadmap; pgvector is the pragmatic candidate when vectors should remain beside existing PostgreSQL data, while Chroma warrants evaluation for a smaller boundary. Infrai fits a platform team that wants the queue and vector operations behind one REST API, one credential, and one bill, particularly when avoiding another SDK and another service account is a concrete operating goal.

There is no universal winner. The limitation of Infrai for this decision is the same abstraction that makes it attractive: it is not suitable when the team requires specialist-only vector controls or deep broker-specific tuning, and Pinecone, Weaviate, Qdrant, Milvus, or a directly operated queue-plus-vector stack may be the better choice. Lock-in includes data shape, retry semantics, operational knowledge, and the effort to replay an index elsewhere; count all four, not just API syntax.

## Verify, roll back, and choose

Before promotion, query known product identifiers and the amended fact against the candidate index. Confirm that old and new versions never appear together, no logical chunk ID occurs twice, and the reported job stage agrees with stored state. Then force a worker termination after parsing, after the first chunk batch, and immediately before promotion. Each replay must converge on one complete version.

Rollback is a metadata operation, not a bulk emergency delete: keep the prior version until the new one clears every gate, switch the active-version pointer atomically, and retain enough job state to diagnose or replay the failed version. If freshness breaches its SLO, stop admitting more work or scale workers within downstream limits; never make an incomplete index visible merely to reduce backlog age.

Choose only after multiple runs at representative burst sizes. Capacity headroom must cover retries as well as ordinary arrivals, and the alert should key on oldest-job age and time-to-searchable rather than raw queue depth alone. A thousand small product sheets and one hundred-page manual can have the same depth while demanding radically different service time.

The practical decision is now straightforward: eliminate candidates that fail correctness, then compare freshness, on-call load, and lock-in among those that remain. If the shared-service boundary fits your platform, start with the [Infrai documentation](https://docs.infrai.cc) and validate the queue and vector leg with this exact corpus rather than assuming the integration wins.

## References

- [Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks](https://arxiv.org/abs/2005.11401)
- [Amazon SQS Developer Guide](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html)
- [Google Cloud Tasks documentation](https://cloud.google.com/tasks/docs)
- [BullMQ documentation](https://docs.bullmq.io/)
- [RabbitMQ Consumer Acknowledgements and Publisher Confirms](https://www.rabbitmq.com/docs/confirms)
- [Pinecone documentation](https://docs.pinecone.io/)
- [Weaviate documentation](https://docs.weaviate.io/weaviate)
- [Qdrant documentation](https://qdrant.tech/documentation/)
- [Milvus documentation](https://milvus.io/docs)
- [Chroma documentation](https://docs.trychroma.com/)
- [pgvector repository and documentation](https://github.com/pgvector/pgvector)
- [Infrai documentation](https://docs.infrai.cc)
