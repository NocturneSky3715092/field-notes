# Catalog Text Summarization Backlog Alerts and Admission Budgets for Replaceable APIs

Consider a proposed page for an e-commerce catalog enrichment service using an LLM API for text summarization: the oldest product description awaiting an accepted summary has crossed its publication deadline. The on-call sees a growing queue, successful API responses, and too few summaries cleared for publication. This is a design scenario, not a reported incident. Its purpose is to expose what an availability dashboard can miss when messy descriptions must become trustworthy product copy.

TL;DR: Choose an LLM API for text summarization by testing whether accepted summaries can meet the catalog's freshness SLO through a provider change. Put admission control, validation, and backlog accounting outside the provider adapter. Compare operational effort and cost per accepted result after that test; a low request price cannot establish which option is best for this workload.

For a Node.js application, the same boundary belongs between the catalog workflow and its outbound provider calls. The Go example below specifies the admission decision independently of that application's SDK. The selection question starts with the publication deadline and the evidence each summary must preserve, because neither a shared language binding nor a similar request body establishes equivalent behavior.

## What should alert before an LLM API misses a catalog text summarization deadline?

The page describes a consequence. An earlier signal should describe the dwindling opportunity to recover: how much time remains before the oldest eligible catalog item is due, compared with how long the current backlog is expected to take to clear. A request-success counter answers a different question. A returned summary may omit a qualifier, add an unsupported material, or fail the catalog's acceptance policy; those are failure cases to test, not claims about any particular provider.

Count accepted work.

Define the denominator before setting an SLO.

One possible policy is to measure whether eligible description revisions receive an accepted summary within their assigned freshness deadline, while tracking quarantined inputs separately so a malformed source cannot disappear into an unexamined exclusion. The business must choose the deadline and permitted miss rate. A provider's response-time target, if one exists, would cover only part of this workflow.

Use a deliberately hypothetical planning case: 600 eligible revisions arrive per minute and the candidate deployment sustains 800 accepted summaries per minute under a representative replay. That leaves 200 per minute for draining accumulated work. A backlog of 4,000 revisions therefore needs roughly 20 minutes to clear while those arrivals continue, assuming those rates remain constant and the queued inputs resemble the replay. These numbers are planning inputs, not a benchmark. If only 15 minutes remain before publication, the recovery signal is already late even though accepted throughput exceeds arrivals.

This distinction matters during a provider switch.

Gross completions can remain steady while acceptance drops, so capacity planning should use measured accepted throughput for the replacement path, including the retries and validation work it actually requires. When throughput cannot exceed arrivals, treat the drain estimate as unbounded until capacity, admission, or workload changes.

## Instrument the revision before instrumenting the request

Start the clock when a description revision becomes eligible for enrichment.

Carry that timestamp through submission, result collection, validation, and publication. Restarting it after a retry would make old work appear fresh; starting it only after submission would hide time spent waiting for capacity. A product identifier alone is insufficient for the proposed acceptance rule because a newer description can arrive while an older revision is still being summarized.

Keep the original description and its revision alongside the candidate output. For example, take the illustrative input, "Blue cotton shirt, adult medium; seller says wash cold; packaging photo unavailable." An acceptance policy could require the summary to preserve the supplied size and care instruction and prohibit unsupported packaging claims. The point is not that these particular fields suit every catalog. It is that the same source and acceptance policy must follow the job across provider adapters, otherwise a migration changes both the execution path and the definition of success.

The instrumentation change is small in concept but consequential in ownership. Record eligible arrivals, accepted publications, validation rejections, pending revisions, and remaining deadline slack at the workflow boundary. Then add request outcomes underneath those measurements for diagnosis. Keep per-revision identifiers in the job record and diagnostic events; use bounded categories such as rejection reason and processing stage for aggregate views.

A compact admission function makes the governing decision reviewable. Here, `pending` includes the candidate revision, `acceptedPerMinute` is a conservative throughput estimate for the active execution path, and `reserve` is the time budget held back for collection, validation, and publication. The caller must recompute these inputs as conditions change.

```go
package admission

import "time"

func CanAdmit(
    now, deadline time.Time,
    pending int,
    acceptedPerMinute float64,
    reserve time.Duration,
) bool {
    if pending < 1 || !(acceptedPerMinute > 0) || reserve < 0 {
        return false
    }
    minutesNeeded := float64(pending) / acceptedPerMinute
    minutesAvailable := deadline.Sub(now).Minutes() - reserve.Minutes()
    return minutesAvailable > 0 && minutesNeeded <= minutesAvailable
}
```

This function estimates completion for admitted work ahead of the candidate; it does not calculate the time required to eliminate a continuously replenished backlog. The earlier recovery calculation subtracts arrivals from service capacity.

Confusing those two estimates is a concrete capacity-planning trap.

The example also assumes a shared deadline class and a queue ordered consistently with the estimate. Different priorities, long descriptions, or separate publishing windows require separate estimates. Rejection should leave a visible deferred job or an explicit policy outcome, with its original clock intact. It must not silently discard a catalog revision.

## Make provider replacement an operational test

A portable adapter needs to return enough information for the workflow to associate an output with its source revision and distinguish a completed candidate from a failed attempt. The catalog service should own acceptance, duplicate-publication prevention, and the decision to stop retrying. Provider-specific submission and collection details belong behind that boundary. This is a proposed division of responsibility, not a claim that different APIs expose identical controls.

Test replacement with a fixed replay of messy descriptions and explicit expected properties. Include conflicting source statements, missing attributes, and a revision that changes while its earlier summary is pending. Check that rejected output remains visible, stale output cannot overwrite a newer accepted revision, and repeating collection cannot publish the same revision twice. A migration rehearsal should also cover uncertain submission outcomes: before repeating work, the adapter needs a defined reconciliation policy for the capabilities actually available.

Buy versus build becomes a question about which responsibilities the platform team is prepared to carry. The table describes responsibilities to verify, rather than promising features for unnamed products.

| Execution arrangement | What the platform still owns | Evidence needed before adoption |
| --- | --- | --- |
| Managed request execution | Queue policy, acceptance, deadlines, publication | Replay acceptance and recovery behavior at the intended concurrency |
| Managed asynchronous execution | Submission tracking, collection, reconciliation, freshness policy | Documented completion window and tested association of results with source revisions |
| Self-hosted execution | Serving capacity and upgrades as well as the catalog workflow | Measured capacity, recovery procedure, and an on-call ownership plan |

The trade-off is control versus maintenance. Keeping more execution machinery in-house gives the team more direct control and more maintenance work. Buying execution can reduce that scope, but the catalog's correctness policy still needs an owner. Neither arrangement earns a portability claim until a second execution path can pass the same acceptance and publication tests.

## Separate asynchronous eligibility from the publication promise

As one documented boundary, the OpenAI Batch API guide describes asynchronous processing with a 24-hour completion window. That is relevant evidence for scheduling: it does not establish that a particular batch will meet an earlier catalog deadline, nor does it establish the acceptance rate of its summaries. The source is listed below. This observation supports a deadline check, not a product recommendation.

For a hypothetical catalog job due in 30 minutes, a documented 24-hour window alone is insufficient evidence to admit that execution path.

That is a scheduling limitation.

This path is not suitable for a deadline policy requiring a documented completion window shorter than the one offered. For work whose deadline accommodates the documented window plus the workflow's own collection and publication reserve, asynchronous execution can be evaluated in the replay. Actual eligibility still depends on the service contract and the catalog's policy.

Budgeting follows acceptance. Compare total execution and operational effort over accepted publications, accounting for discarded candidates, retries, queue maintenance, and the staff effort required to keep the path usable. Keep request pricing as an input to that comparison. It should not replace the freshness and portability tests, and no universal cheapest choice follows from the evidence here.

## Tune the warning against the cost of a false page

Return to the proposed alert. Page when the predicted recovery margin threatens the freshness objective and an operator has a useful intervention, such as pausing lower-priority admissions or activating a replacement path that has already passed the replay. A rising backlog with ample deadline slack may warrant a dashboard signal; it need not automatically wake someone.

Thresholds need replay too.

Evaluate the proposed warning against bursts, validation slowdowns, and deliberate capacity reductions, then inspect how much intervention time it provides and how often it calls for action that cannot improve the outcome. Choose any persistence window with the publication deadline in view: waiting for a stable signal consumes recovery time.

False positives have a cost. A threshold set too close to ordinary variation can trigger needless provider switches, extra validation work, and repeated interruptions; a threshold set too late consumes the time needed to recover. The useful selection outcome is an execution path the team can replace while still meeting its catalog freshness objective, with an alert policy that buys time to act.

## References

The batch-processing guide supports the asynchronous completion-window boundary discussed above. The workload figures, input description, admission policy, and alert scenario are explicitly illustrative design examples; they are not measured service results or accounts of production incidents.

## Further reading

- OpenAI Batch API guide: https://platform.openai.com/docs/guides/batch
