# Large-Volume User Content Moderation — Batch Review Capacity for Logistics Calls

The cheapest workable way to moderate a large volume of user content is batch classification behind a schema gate, with a bounded review queue for ambiguous results; token cost matters only after the accepted output is correct. For a logistics team extracting owners, due dates, and follow-ups from sales calls, structured output correctness is the admission ticket, because a low-cost result that corrupts the CRM has negative value. **A cheap invalid action is still an incident.**

TL;DR: establish a deterministic schema gate, measure input and output tokens with the tokenizer used in production, replay a labeled evaluation set before deployment, and size the review queue from arrival and service rates. Optimize total cost per accepted action, not model spend in isolation.

## How should batch classification moderate large-volume user content?

The production scenario I use for this decision is deliberately bounded: recorded logistics sales calls arrive after transcription, a classifier proposes CRM actions, and a person reviews the cases the system cannot accept safely. The actions are narrow: create a follow-up, assign an owner, attach a due date, or record that no action was requested. Content screening is part of admission because abusive text, prompt-like instructions inside a transcript, and sensitive fragments must not flow blindly into downstream fields.

The dangerous failure is not a dramatic model outage. It is a plausible object with the wrong meaning: an invented due date, an owner copied from a customer quote, an unsupported action type, or a low-confidence result that bypasses review. Those errors can survive syntax checks and quietly create operational work. The invariant is therefore stronger than “valid JSON”: every accepted object must satisfy a closed action vocabulary, required-field rules, evidence linkage, and an explicit review decision.

I would define two service indicators before debating batch discounts: the proportion of accepted actions that pass a labeled correctness audit, and the age of the oldest review item. One protects the CRM; the other protects the humans. A third indicator, schema-rejection rate, is diagnostic rather than a customer outcome, but a sudden rise is a useful deployment signal.

Short queues lie.

A backlog may look harmless at noon and become unrecoverable after a campaign import. Capacity planning needs an explicit envelope. Suppose, as a planning example rather than a benchmark, 48,000 calls arrive over an eight-hour window, 6% require review, and one reviewer closes 24 items per hour. That creates 2,880 reviews and 120 reviewer-hours. Ten reviewers provide 240 reviews per hour, so arrival bursts and breaks still matter even though the average arithmetic appears comfortable. Replace every number with measured traffic before staffing or setting an SLO.

## Can token counting predict the real bill?

It predicts one component. Count the exact serialized request and expected response with the production tokenizer, then retain those counts beside the result. Character counts and word counts are weak proxies because tokenization depends on the selected model and encoding. The estimate should separate input tokens, output tokens, retries, evaluation traffic, storage, and human review. Do not hide review labor inside a vague overhead percentage; it is usually driven by the escalation rate and handling time, not tokens.

Use an equation that exposes those levers:

`total workload = initial classifications + safe retries + evaluation replays + human reviews`

For money, apply the current contracted rates outside the article: input tokens times the applicable input rate, output tokens times the applicable output rate, plus non-model infrastructure and review labor. Rates change. The durable decision variable is **cost per schema-valid, audit-accepted action**, with rejected and retried work left in the denominator’s cost but not its success count.

Token reduction still helps, within limits. Remove repeated boilerplate, transmit only transcript spans needed for the decision, cap output to the closed schema, and keep policy text versioned so the same evaluation can be replayed. Do not trim away speaker labels, negation, dates, or nearby evidence merely to improve a token chart. In this workload, a shorter prompt that changes who owns a follow-up has failed the economic test.

## Gate the write path, not the dashboard

The preventative control belongs between inference and CRM mutation. This Go example validates a decoded result, requires evidence for every proposed action, and forces uncertain or policy-flagged cases into review. The thresholds are configuration examples, not universal measurements.

```go
package actiongate

import (
    "errors"
    "fmt"
    "strings"
    "time"
)

type Action struct {
    Kind       string
    Owner      string
    DueDate    string
    Evidence   string
    Confidence float64
}

type Result struct {
    CallID      string
    PolicyFlags []string
    Actions     []Action
}

type Decision string

const (
    Accept Decision = "accept"
    Review Decision = "review"
    Reject Decision = "reject"
)

var allowedKinds = map[string]bool{
    "follow_up":       true,
    "assign_owner":    true,
    "record_no_action": true,
}

func Gate(r Result, minConfidence float64) (Decision, error) {
    if strings.TrimSpace(r.CallID) == "" {
        return Reject, errors.New("missing call_id")
    }
    if len(r.PolicyFlags) > 0 {
        return Review, nil
    }
    for i, a := range r.Actions {
        if !allowedKinds[a.Kind] {
            return Reject, fmt.Errorf("action %d has unknown kind", i)
        }
        if strings.TrimSpace(a.Evidence) == "" {
            return Reject, fmt.Errorf("action %d has no evidence", i)
        }
        if a.DueDate != "" {
            if _, err := time.Parse("2006-01-02", a.DueDate); err != nil {
                return Reject, fmt.Errorf("action %d has invalid due_date", i)
            }
        }
        if a.Confidence < minConfidence {
            return Review, nil
        }
    }
    return Accept, nil
}
```

The gate is intentionally boring. It does not establish semantic truth, so a labeled audit must sample accepted results as well as reviewed ones; otherwise the team measures only cases the classifier already admitted were hard. Persist the prompt version, policy version, tokenizer identity, model identifier, schema version, token counts, decision, and retry attempt with each result. That record supports replay and makes a regression attributable.

Writes also need an idempotency design. RFC 9110 distinguishes idempotent methods and explains the retry implications when a connection fails before a response is read. An application-level operation key derived from the call ID, action type, and schema version can prevent one logical action from becoming two CRM mutations, but the receiving store must enforce uniqueness. A retry loop by itself is not that enforcement.

## Buy, build, or keep humans in the loop?

The choice is not binary. Managed inference, self-hosted inference, deterministic rules, and manual review can occupy different stages. I use a buy-versus-build table to expose who carries the pager and who owns correctness; product names add little here.

| Path | Best fit | Main constraint | Operational owner |
|---|---|---|---|
| Deterministic rules first | Stable phrases and closed vocabularies | Recall falls on varied language | Application team |
| Managed inference | Variable demand and a small platform team | External dependency and portability work | Provider plus platform team |
| Self-hosted inference | Steady, measured load with specialized controls | Capacity, upgrades, and on-call load | Platform team |
| Human review | Ambiguous, sensitive, or high-impact actions | Queue delay and staffing capacity | Operations team |

No row wins universally. A small team with bursty call volume may rationally buy inference capacity while retaining its own schema gate and evaluation corpus. A team with predictable utilization and established accelerator operations may accept self-hosting. Deterministic rules should claim the easy cases when their false-positive behavior is measurable. Human review remains a control plane for ambiguity, not an unbounded sink for every low-quality response.

The rollout sequence follows the blast radius: shadow the classifier without writes, replay a labeled set, enable review-only output, then allow a narrowly scoped action type after its acceptance audit meets the target. Canary by prompt and schema version. Roll back by stopping admission of new writes, while leaving already queued reviews traceable; changing the prompt without changing the version destroys that option.

## When does this design not apply?

The main limitation is semantic uncertainty: this gate can reject malformed actions, but it cannot prove that a fluent action matches the call. That trade-off rules out using the pipeline for irreversible safety, legal, employment, or financial decisions without domain-specific controls and accountable human authority. It is also a poor fit when volume is low enough for direct review, or when a deterministic parser can produce the required CRM action with better measured accuracy.

Batching is inappropriate when the response deadline is shorter than the batch window. Likewise, a confidence threshold is not portable across prompts or models; calibrate it against labeled examples and re-evaluate it after any material change. Prompt-engineering patterns can improve task framing, but they do not replace validation, measurement, or an authorization boundary.

The decision rule is plain: accept automation only while structured correctness meets its SLO and the review queue stays inside its age objective under a measured peak. Then optimize tokens, batch size, and placement. Reversing that order produces attractive unit economics for work the business cannot trust.

## Sources

References used for the protocol and prompt-design boundaries:

- RFC 9110, HTTP Semantics: https://www.rfc-editor.org/rfc/rfc9110
- Prompt Engineering Guide: https://www.promptingguide.ai
