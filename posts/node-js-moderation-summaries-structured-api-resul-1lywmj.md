# Node.js Moderation Summaries: Structured API Results for Async Document Review

Short answer: treat moderation-report summarization as a durable asynchronous job, require a versioned structured result for every document, and export only after count, schema, and provenance checks pass. The important design decision is correctness of the result contract, not whether a request can be made to wait a little longer.

That distinction matters in media systems. A moderation report can contain a short accusation, a long transcript excerpt, or several languages in one record. A summary that reads well but drops the report ID, invents a category, or cannot be reconciled with its source is not a useful summary. It is an untracked review decision.

## The failure signal is a trustworthy-looking wrong result

The first dangerous failure is not a timeout. It is a completed job whose output looks plausible while its structure is wrong. A human reviewer can read a sentence and miss that `report_id` is absent; a downstream queue cannot. The ingestion boundary therefore needs a schema with required identifiers, an explicit classification set, a summary field, and a way to represent uncertainty without silently converting it into a label.

Keep the original report immutable and attach a content hash to the work item. Store the prompt or instruction version, schema version, model configuration, and submission timestamp beside that hash. This gives an operator something concrete to compare when a batch contains fewer results than inputs. It also prevents a later export from becoming the only record of what was submitted.

Do not measure this workload with requests per second alone. Capacity follows input length, output allowance, concurrent jobs, and retry amplification. Sample the actual report corpus, including the long tail, then set an SLO for the time from accepted job to reconciled export. I'm not sure what concurrency ceiling your corpus can sustain until those distributions are measured; an average document is not a capacity plan.

Three words: queue age matters.

## How should a Node.js API process multiple documents as an async job?

The Node.js API should validate a manifest, create a stable client job key, enqueue one durable work record, and return an acknowledgment without holding the request open for model latency. A worker claims that record, submits the provider-specific request through an adapter, records the remote identifier, and polls with a bounded schedule. The browser or calling service reads your job state, not an unbounded remote polling loop.

The adapter boundary is deliberate. It keeps authentication, request translation, and provider-specific lifecycle names out of the moderation domain. A queue record can remain provider-neutral:

```go
type WorkItem struct {
	ClientJobID    string
	RemoteJobID    string
	ManifestHash   string
	InstructionRev string
	SchemaRev      string
	State          string
	Attempts       int
}

type SummaryResult struct {
	ReportID   string   `json:"report_id"`
	Category   string   `json:"category"`
	Summary    string   `json:"summary"`
	Confidence float64  `json:"confidence"`
	Evidence   []string `json:"evidence"`
}
```

The production contract should reject unknown or missing identifiers before export. It should also make a partial outcome visible. A batch with 99 valid summaries and one malformed item is not automatically successful just because the remote job says complete; the policy may allow a review queue for that one item, but the policy must be explicit.

Use an idempotency key derived from the client job key when the upstream API supports one. If it does not, persist the submission attempt and reconcile by the remote job identifier before retrying. Never use a random key generated inside a retry loop. That turns a network ambiguity into duplicate work.

## What should the export and verification boundary prove?

Export is a promotion step, not a file download. Before an artifact becomes visible to reviewers, verify that every input report has exactly one terminal result, every result has the expected schema version, every `report_id` belongs to the manifest, and the manifest hash matches the batch record. Consider the failure chain that makes this necessary: the API accepts 500 reports, the worker records a remote job ID, a network interruption hides the first poll response, and a retry later returns 499 parseable summaries. If the exporter checks only the remote terminal state, it can publish a file that looks healthy while silently dropping one report. If it checks only the row count, a duplicated ID can still occupy the missing row. The ledger, manifest hash, per-item identity check, and explicit partial-result policy close those different gaps at different layers. Write to a temporary object or file, flush it, then promote it under a stable job path. A consumer should never have to guess whether a file is still being assembled.

Here is a small reconciliation function. It is intentionally boring: its job is to make the success condition executable, not to hide disagreement behind a best-effort export.

```go
package main

import "fmt"

func Reconcile(expected []string, results []SummaryResult) error {
	want := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		if _, exists := want[id]; exists {
			return fmt.Errorf("duplicate input report_id %q", id)
		}
		want[id] = struct{}{}
	}

	seen := make(map[string]struct{}, len(results))
	for _, result := range results {
		if result.ReportID == "" || result.Summary == "" {
			return fmt.Errorf("result is missing report_id or summary")
		}
		if _, exists := want[result.ReportID]; !exists {
			return fmt.Errorf("unexpected report_id %q", result.ReportID)
		}
		if _, exists := seen[result.ReportID]; exists {
			return fmt.Errorf("duplicate result for report_id %q", result.ReportID)
		}
		seen[result.ReportID] = struct{}{}
	}
	if len(seen) != len(want) {
		return fmt.Errorf("result count %d does not match input count %d", len(seen), len(want))
	}
	return nil
}
```

A schema validator belongs beside this function, and a fixture set should include truncated text, an empty report, an unknown category, duplicate IDs, and a valid multilingual report. The exact category vocabulary is a policy decision for the moderation team. Do not let a model invent it during implementation.

## Capacity, buy-versus-build, and the operational catch

There is no single correct control plane. A managed API reduces the amount of execution infrastructure the platform team owns, but it adds a remote lifecycle contract, quota behavior, data-governance review, and lock-in at the adapter boundary. A self-hosted worker offers more control, while the team inherits capacity planning, model rollout, hardware utilization, and the on-call path. An existing cloud AI control plane may fit governance better than either choice, but it can also make portability harder.

| Choice | Fits when | The catch |
| --- | --- | --- |
| Managed asynchronous API | The team wants to own the job ledger and validation while outsourcing model execution | Remote quotas, retention rules, and lifecycle semantics become dependencies |
| Existing cloud control plane | Identity, audit, and data locality already live there | Provider-specific request shapes can widen the adapter and migration cost |
| Self-hosted model worker | Data control and predictable execution justify owning the serving plane | Hardware, upgrades, capacity, and incident response stay with the team |
| Local queue plus synchronous calls | The document set is small and latency is tightly bounded | It becomes unsuitable as queue age or retry amplification threatens the SLO |

The right choice is the one whose failure modes the team can operate. A managed path is not suitable when policy forbids external processing or when its retention and regional guarantees do not meet the review workflow. Self-hosting is a poor fit when the team cannot staff model-serving operations. Stick with an existing platform when its identity, observability, and audit controls outweigh the benefit of a new integration.

Budget concurrency from the p95 and p99 input-size bands, not the median. Put a maximum item size in the manifest, split oversized reports before submission, and cap retries so a throttled queue does not create more load than the original batch. Record queue age, remote-job age, validation failures, duplicate submissions, export promotion failures, and the count of results routed to human review. Those are SLO signals; a green HTTP response is not.

## Roll out, verify, and roll back without losing evidence

Start with a representative fixture batch and compare structured outputs against human-reviewed decisions. Keep a fixed evaluation set outside the live queue so a prompt or schema revision can be compared with the previous revision. Release behind a routing switch, increase the batch-size band gradually, and watch the completion objective together with malformed-output rate. Your mileage may vary because moderation taxonomies and report lengths differ sharply by product surface.

Rollback should stop new submissions to the new path while allowing already accepted jobs to reach a recorded terminal state. Route new work to the prior adapter, retain the manifest and content hash, and quarantine exports until reconciliation finishes. If a result has already been promoted, use the stable client job key to identify it; do not delete the ledger to make the dashboard look clean.

The practical rule is simple: a summary is publishable only when its identity, structure, provenance, and completeness are all testable.

Everything else belongs in a review queue.

## References

- OpenAI `tiktoken` repository: https://github.com/openai/tiktoken
- ElevenLabs documentation: https://elevenlabs.io/docs

## Further reading

- https://github.com/openai/tiktoken
- https://elevenlabs.io/docs
