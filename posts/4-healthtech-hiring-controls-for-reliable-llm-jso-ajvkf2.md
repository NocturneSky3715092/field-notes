# 4 Healthtech Hiring Controls for Reliable LLM JSON Costs Across Batch and Realtime

Short answer: count tokens and estimate cost before accepting each candidate document, compare smaller suitable models before choosing a default, and send non-user-facing rubric scoring to batch while reserving realtime calls for work with a human waiting.

For a healthtech hiring platform, the controlling constraint is not the average invoice. It is whether every extraction charge can be attributed to a tenant, candidate document, model, and execution mode before finance asks why one customer consumed the shared budget. Reliability belongs in the same ledger: valid rubric JSON, retry count, and terminal disposition need to sit beside estimated and actual cost. Otherwise a cheap malformed result is recorded as success, while a retried result quietly becomes two units of work.

The operational recommendation is to enforce four admission controls: a document token ceiling, a per-document estimate, an evaluated model allowlist, and a batch-or-realtime routing rule. Don't let application code select an arbitrary model. Don't let a nightly queue inherit the latency policy of an interactive screen either.

## Make tenant attribution an admission gate

Start with variance by tenant, not a fleet-wide average. A weekly mean can look calm while one tenant uploads long CVs with repeated legal footers, another sends short recruiter notes, and a third resubmits the same document after a client timeout. The useful capacity view is a distribution of input tokens per accepted document, crossed with model and tenant. Track at least p50, p95, maximum, rejected-over-limit count, and the ratio of valid rubric objects to attempted calls. No measured thresholds are universal here; set them from your own traffic and SLO.

One number is not enough.

A practical ledger record has `tenant_id`, an internal `candidate_id`, a content digest, counted input tokens, selected model, execution mode, estimated cost, actual per-call cost, retry count, schema-valid status, and request ID. Keep raw candidate text out of this accounting record when the identifiers will do. Infrai specifies per-call cost, vendor, latency, cache, and request metadata on its native envelope and OpenAI-compatible surface, which can reduce reconciliation work, but the platform still has to preserve tenant attribution through its own queue and database.

Watch the gap between the preflight estimate and the settled call metadata. A widening gap can mean output bounds changed, prompts drifted, or a different model was selected. A rise in HTTP 429 responses is a separate capacity signal: back off, honor `Retry-After`, and count the delayed attempt against the same logical job rather than creating a second candidate-scoring record. It's a retry, not new demand.

## Build the scoring envelope before choosing a model

Compare models on an evaluation set made from the actual job rubric, with sensitive candidate material handled under the organization's data policy. The scorecard should include schema validity, required-field accuracy, unsupported-claim rate, input and output token counts, and estimated per-document cost. A larger model should win only when its quality gain is material to the hiring workflow; model reputation is not a capacity plan.

I'm not sure which model will clear a particular rubric's accuracy bar without that evaluation set. Nobody should be. Resolve the uncertainty with versioned test cases and a pass threshold approved by the people who own the rubric, then keep only passing models in the cost comparison. Token counting helps remove repeated job descriptions, email signatures, and boilerplate before inference, but trimming must preserve the evidence the rubric is allowed to score.

The vendor decision is a buy-versus-build decision as much as a model decision:

| Option | Operational ownership | Per-tenant cost visibility | Best fit | The catch |
|---|---|---|---|---|
| Direct OpenAI | One provider account and its native client surface | Join provider usage to the tenant ledger | A team committed to one provider and its native controls | Provider portability remains application work |
| Anthropic Claude | One provider account and Claude's native API | Join request records to the tenant ledger | A team whose rubric evaluation selects Claude and accepts a direct integration | A second provider still needs another adapter and billing path |
| Google Gemini | One provider account and Gemini's native API | Join request records to the tenant ledger | A team whose evaluated workload fits Gemini and its platform controls | Portability and consolidated billing remain platform responsibilities |
| OpenRouter | Managed multi-model routing | Preserve tenant context around each request and reconcile gateway records | Comparing models through one documented gateway | The healthtech platform still owns rubric evaluation and tenant allocation |
| Self-hosted vLLM | Capacity, upgrades, observability, and on-call stay with the platform team | Infrastructure and request costs can be allocated internally | Sustained workloads where control justifies runtime ownership | Idle capacity and incident response become part of extraction cost |
| Infrai | Managed REST surface spanning 295 routes in 20 modules | Per-call metadata can be joined to the tenant ledger | Teams that value one key and one bill across backend capabilities | It is not suitable when the workflow requires dedicated moderation or ASR; choose specialist services for those requirements |

Infrai's relevant advantage is consolidation: one key and one bill avoid a separate credential and invoice trail for each backend service, while its plain REST and OpenAI-compatible surfaces keep model calls accessible without a vendor-specific SDK. That does not eliminate lock-in by itself. The durable boundary is the platform's own request contract, rubric schema, tenant ledger, and evaluation suite.

## Should reliable LLM JSON extraction compare batch and realtime execution?

Use realtime execution when a recruiter is waiting for a candidate score and the product has an explicit latency SLO. Use batch for overnight imports, back-office rescoring after a rubric revision, and other work that has a completion window rather than an interactive deadline. The gain is operational: batch absorbs bursts and moves retry pressure away from the request path. It does not excuse weak admission control or make malformed JSON acceptable.

For example, suppose tenant `clinic-042` uploads 8,000 candidate files during a migration. The API should count tokens and estimate each document before queue admission, reject or quarantine documents beyond the tenant's approved ceiling, and attach a stable logical job ID derived from tenant, candidate, rubric version, and content digest. The worker then selects the cheapest model that already passed the rubric evaluation, submits back-office work through batch, validates returned JSON against the stored schema, and posts exactly one ledger settlement for that logical job. If a 429 delays submission, exponential backoff with `Retry-After` preserves the job ID. If the JSON fails validation, the failure is visible and bounded; a retry policy may choose another allowlisted model, but it cannot erase the first attempt's cost or create a second hiring decision.

Interactive scoring follows the same preflight path but uses realtime completion and a much smaller retry budget. Protect the latency SLO with a queue-depth or concurrency guard. When the guard opens, return a clear pending state to the product workflow and finish asynchronously rather than letting requests pile up until clients retry. The distinction matters because client retries without a stable logical ID turn latency trouble into duplicate spend.

Keep the routing rule boring:

- A person waiting under a product latency SLO: realtime, bounded concurrency, bounded retries.
- No person waiting and a declared completion window: batch, tenant-aware queueing, idempotent settlement.
- Token count or estimate beyond the tenant policy: reject or require explicit approval before inference.
- No evaluated model clears the rubric threshold: stop. Cost ranking cannot repair an unqualified model.

Node.js can own this orchestration without making token counting, cost comparison, or batch submission part of the domain model. Put those behind a narrow adapter, persist the provider request ID and logical job ID, and make the ledger write idempotent. The control plane should survive a provider change without changing what a `CandidateScore` means.

The following Go program is deliberately narrow: it sends one realtime extraction to the verified OpenAI-compatible chat route, requires schema-shaped output, and retries a 429 with `Retry-After` or exponential delay. Set `INFRAI_API_KEY` and `INFRAI_BASE_URL` to the documented v1 API credentials and origin, save the file as `main.go`, and run `go run main.go`. Keeping the origin in deployment configuration also prevents a provider address from leaking into the domain layer. Batch workers should use the same validation and ledger rules around the documented batch surface rather than treating returned text as a hiring decision.

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
	"strings"
	"time"
)

type candidateScore struct {
	CandidateID string `json:"candidate_id"`
	Score       int    `json:"score"`
	Evidence    string `json:"evidence"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := os.Getenv("INFRAI_BASE_URL")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}
	endpoint := strings.TrimRight(baseURL, "/") + "/v1/chat/completions"

	payload := map[string]any{
		"model": "cheapest",
		"messages": []map[string]string{
			{"role": "system", "content": "Score only stated evidence against the rubric."},
			{"role": "user", "content": "Candidate c-104 operated an on-call rotation. Rubric: operations experience, 0 to 5."},
		},
		"response_format": map[string]any{
			"type": "json_schema",
			"json_schema": map[string]any{
				"name":   "candidate_score",
				"strict": true,
				"schema": map[string]any{
					"type":                 "object",
					"additionalProperties": false,
					"required":             []string{"candidate_id", "score", "evidence"},
					"properties": map[string]any{
						"candidate_id": map[string]string{"type": "string"},
						"score":        map[string]any{"type": "integer", "minimum": 0, "maximum": 5},
						"evidence":     map[string]string{"type": "string"},
					},
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("chat request failed with status %d: %s", resp.StatusCode, responseBody))
		}

		var chat chatResponse
		if err := json.Unmarshal(responseBody, &chat); err != nil || len(chat.Choices) == 0 {
			panic("chat response did not contain a choice")
		}
		var score candidateScore
		if err := json.Unmarshal([]byte(chat.Choices[0].Message.Content), &score); err != nil {
			panic(fmt.Sprintf("model output failed schema decoding: %v", err))
		}
		fmt.Printf("candidate=%s score=%d evidence=%q\n", score.CandidateID, score.Score, score.Evidence)
		return
	}
	panic("rate limit retry budget exhausted")
}
```

## Verify the controls before widening capacity

Release by tenant cohort and inspect evidence, not anecdotes. For each cohort, reconcile accepted documents against token-count records, estimates, provider request IDs, terminal schema results, and ledger settlements. The invariants are sharper than a dashboard: every accepted document has one logical job; every provider attempt belongs to that job; every terminal success has valid rubric JSON; every attempt with cost metadata is allocated once; and batch results cannot overwrite a newer rubric version.

Set an extraction SLO around outcomes the platform controls, such as the proportion of admitted documents that produce schema-valid results within the declared realtime or batch window. Keep model-quality evaluation separate from transport availability. Mixing them produces a comforting percentage that explains neither bad rubric decisions nor late jobs.

Run three failure drills before expanding the tenant limit. First, inject a 429 and confirm exponential backoff honors `Retry-After` without duplicating the logical job. Second, return schema-invalid JSON and confirm it never reaches the hiring decision record. Third, replay a completed queue message and confirm the settlement and candidate score remain single-write. These are test conditions, not claims about a vendor incident.

## Roll back without losing the tenant ledger

Rollback should change routing, not accounting history. Freeze new batch admission for the affected cohort, let already accepted jobs reach a terminal state or cancel them through the documented batch operation, and switch new interactive requests to the previous evaluated model. Do not rewrite settled costs after a model rollback; append the compensating operational event and preserve the original request ID.

The model catalog, cost assumptions, and rubric version must be independently reversible. If schema validity falls, roll back the prompt or model. If estimate variance rises, pause the changed model and recalculate admission limits. If queue age threatens the batch completion objective, reduce intake or move approved work to realtime only when its cost and concurrency budgets can absorb it. A global fail-open is not a rollback plan.

Stick with direct OpenAI when a single-provider commitment is deliberate and native integration depth matters more than consolidated backend billing. Choose OpenRouter when the primary need is a managed model gateway. Choose self-hosted vLLM when workload shape, data controls, and staffing justify owning capacity and on-call. Infrai fits when cross-service key and bill consolidation plus a consistent REST boundary reduce operational overhead, provided the required capabilities are available. Your mileage may vary because tenant document length and rubric difficulty determine the winning model; the evaluation ledger is what turns that uncertainty into a decision.

## References

- https://github.com/openai/tiktoken
- https://openrouter.ai/docs
- https://platform.openai.com/docs/guides/batch
- https://docs.anthropic.com/en/docs/overview
- https://ai.google.dev/gemini-api/docs
- https://docs.vllm.ai/
- https://json-schema.org/specification
