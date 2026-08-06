# SLO-Driven API Selection: LLM JSON Schema Safety for In-App Chat Traffic

**Short answer:** The best API for safe, basic moderation in an in-app chatbot without a dedicated moderation endpoint is one that enforces an LLM JSON schema and sustains the extra call inside your latency and availability budgets.

There is no universally best API for basic moderation when an in-app chatbot has no dedicated moderation endpoint. I would select the runtime only after testing whether it reliably returns a small JSON decision, exposes enough usage and request metadata to operate the gate, and permits an explicit model version. The architecture matters more than the logo: classify the user turn, validate the result locally, allow or refuse, then generate only when allowed.

Don't confuse this with a complete trust-and-safety program. An LLM judge is a useful first control for a bounded product surface, but prompt injection, sensitive-data disclosure, insecure output handling, abuse investigation, and policy appeals still need separate controls. My platform SLO is for the whole user transaction — two model calls, local validation, logs, and the refusal path — rather than for a provider's request in isolation.

## What should the API decision record contain?

Start with the artifact you need during an incident: a compact verdict record. Mine has a policy version, decision, matched categories, confidence band, stable request ID, classifier model identifier, and a hash of the input. I don't log raw chat text by default because the moderation trail can quietly become a second store of secrets and personal data. Access to any sampled payloads has its own retention period and audit boundary.

The schema should make ambiguous output impossible to accept. Keep the decision enum small, reject unknown fields, cap category count, and define a conservative result for validation failure. Free-form explanations are tempting, but they add tokens, leak fragments of the prompt into logs, and make dashboards harder to aggregate. A short reason code is enough for automation; a separately controlled review tool can carry human notes.

Keep it small.

**Fail closed on a malformed or missing verdict, but distinguish that refusal from a policy refusal.** Users can receive similar wording, while operators need separate counters. Otherwise a model-format regression looks like a sudden wave of harmful traffic. I page on sustained gate unavailability, not on individual policy matches, and I track malformed output as its own service-level indicator.

Capacity planning is blunt here. Every accepted user turn consumes one classification call plus one generation call, while rejected turns consume only classification. The arrival-rate model therefore needs the observed allow ratio, both latency distributions, token limits, retry policy, and concurrency caps. A provider that looks fast on generation alone may miss the end-to-end budget once the serial gate is included. Measure p50, p95, and p99 at the transaction boundary; averages hide the queueing pain that wakes people up.

## How should an in-app chatbot use an LLM JSON schema without a moderation endpoint?

Treat classification as a narrow typed function, not as a second chatbot. The system instruction should contain the policy taxonomy and tell the model to classify only the supplied content. Place untrusted text in a dedicated input field, set a strict output schema through the API's supported structured-output mechanism, and validate the returned bytes again in your process. Schema support is an admission criterion because “please answer in JSON” is not a contract.

The safe sequence is: assign a request ID, evaluate the input, parse and validate the verdict, persist the decision once, then either return a refusal or call generation. The generation result still passes through deterministic output controls appropriate to the application: authorization checks, URL rules, secret scanning, escaping, and tool-call allowlists. Input screening cannot prove that generated output is safe — prompt injection is one reason the two boundaries must remain distinct.

Here is the shape I use at the application boundary. The provider adapter implements `Classify`; this code owns validation, conservative defaults, and idempotent recording. The enum and category allowlist are deliberately boring.

```go
package safety

import (
	"context"
	"errors"
	"slices"
)

type Verdict struct {
	Decision  string   `json:"decision"`
	Categories []string `json:"categories"`
	Policy    string   `json:"policy_version"`
}

type ModerationModel interface {
	Classify(ctx context.Context, policyVersion, userText string) ([]byte, error)
}

type VerdictParser interface {
	ParseAndValidate(raw []byte) (Verdict, error)
}

type DecisionStore interface {
	PutOnce(ctx context.Context, requestID string, verdict Verdict) error
}

var allowedCategories = []string{"abuse", "self_harm", "sexual", "violence"}

func Screen(ctx context.Context, model ModerationModel, parser VerdictParser, store DecisionStore, requestID, text string) (Verdict, error) {
	raw, err := model.Classify(ctx, "chat-policy-v3", text)
	if err != nil {
		return Verdict{Decision: "refuse", Policy: "chat-policy-v3"}, err
	}

	verdict, err := parser.ParseAndValidate(raw)
	if err != nil || (verdict.Decision != "allow" && verdict.Decision != "refuse") {
		return Verdict{Decision: "refuse", Policy: "chat-policy-v3"}, errors.New("invalid verdict")
	}
	for _, category := range verdict.Categories {
		if !slices.Contains(allowedCategories, category) {
			return Verdict{Decision: "refuse", Policy: "chat-policy-v3"}, errors.New("unknown category")
		}
	}
	if err := store.PutOnce(ctx, requestID, verdict); err != nil {
		return Verdict{Decision: "refuse", Policy: "chat-policy-v3"}, err
	}
	return verdict, nil
}
```

The parser behind this interface should use a real JSON Schema validator, not hand-written field checks alone; the loop above demonstrates the application-level invariant that category values also belong to the deployed policy. Keep the exact schema and prompt under version control, and store their version beside every decision.

## Build, buy, or combine the safety gate

I make this choice with an on-call ledger, not a feature checklist. A managed model API removes accelerator scheduling and model serving from my team's queue, yet it introduces external rate limits, data-handling review, and a dependency whose latency is now serial with generation. Self-hosting gives tighter control over weights, placement, and traffic, but the team owns capacity headroom, rollouts, evaluation, observability, and urgent policy updates. A hybrid adapter can preserve an exit path, though portable request and response types usually target the least common denominator.

| Option | Operational upside | Cost and lock-in surface | Not suitable when |
|---|---|---|---|
| Managed structured-output API | Small serving burden; quick model evaluation | Per-use spend, provider quotas, model-specific schema behavior | Data residency or dependency policy forbids the external call |
| Self-hosted classifier | Direct control of placement, version, and reserved capacity | Accelerator utilization, patching, rollout, and on-call ownership | The team cannot staff model-serving operations |
| Adapter over multiple runtimes | Easier controlled migration and comparative testing | More conformance tests and lowest-common-denominator features | One runtime is mandated and portability has no credible value |
| Deterministic rules plus LLM | Fast obvious matches and a typed judgment for context | Two policy systems must stay aligned | The taxonomy cannot be expressed or evaluated consistently |

The catch is that JSON Schema compliance says nothing about policy quality. Before selecting an API, run the same versioned evaluation set through each candidate and score category recall, false refusals, malformed responses, latency, and token use. Include benign slang, quoted harmful text, multilingual turns, obfuscated terms, long context, and direct attempts to rewrite the classifier's instruction. I'm not sure why teams still accept a single aggregate accuracy number; it conceals the category that will dominate support tickets. Your mileage may vary, especially across languages and short ambiguous messages.

Stick with deterministic rules when the policy is small, explicit, and costly to misinterpret. Use a trained specialist classifier when stable high-volume categories justify its evaluation and serving work. An LLM schema gate fits the middle: contextual judgment is useful, traffic is manageable, and the product can tolerate a conservative refusal during uncertainty. None of these choices removes human escalation for credible threats, appeals, or policy changes.

## Verify retries, deployment, and rollback under load

I hit a duplicate-write bug when a retry that looked harmless fired after the database commit: the caller repeated the operation, and one user turn produced 2 writes with different audit IDs. We had made the model call retryable but not the decision side effect. The fix in the design was a request-scoped idempotency key carried through classification, storage, and generation, with a uniqueness constraint at the write boundary — the retry can repeat evaluation, but it cannot create a second durable decision.

Retries are writes.

Test that invariant before debating model quality. Inject client timeouts before and after each side effect, duplicate delivery, malformed JSON, unknown enum values, context cancellation, and a slow dependency. Confirm that the system refuses conservatively, increments the right counter, preserves one audit record, and never sends blocked input to generation. Don't blindly retry policy refusals or schema failures. Retry only transient transport outcomes within a small deadline and retry budget, using the same idempotency key.

Deploy policy, schema, prompt, and model changes as one versioned unit — then shadow them against recorded, access-controlled test cases before they influence users. I want category-level confusion matrices, malformed-output rate, refusal rate, gate latency, and generation suppression rate split by version. A canary should have an automatic stop condition tied to the transaction SLO and a manual review queue for changed decisions. Keep the previous unit deployable until the audit window closes.

Rollback is a policy operation as much as a software operation. Reverting application code while leaving the new prompt or schema active creates an untested combination, so the runbook names the exact version tuple and its owner. During classifier unavailability, a high-risk surface should fail closed; a low-risk internal assistant may switch to a restricted mode with no tools, no sensitive retrieval, and a clear operator alert. That choice belongs in the threat model before launch, because an incident is a poor time to negotiate risk appetite.

Done right, this is a modest control with explicit limits. The best API is the one that passes your corpus, schema-conformance, latency, data-governance, and failure-injection tests while leaving the team an affordable on-call burden and a credible rollback path.

## References

- OWASP Top 10 for Large Language Model Applications: https://owasp.org/www-project-top-10-for-large-language-model-applications/
- OpenRouter documentation, used as one example of a general-purpose model API surface: https://openrouter.ai/docs
