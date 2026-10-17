# Runaway API Spend: Hard Caps and Early Alerts for Customer Billing

The budget alert page says one tutoring customer has consumed 78% of its monthly allowance before the school day is over, while the hard spend cap is close enough to matter. The on-call engineer can see the customer ID, metered units, alert threshold, and budget period; there is still time to inspect the workload before service is refused.

**Short answer:** set a hard cap at the number the customer cannot exceed, then put an alert threshold comfortably below it. An alert alone cannot stop a runaway loop. Only the system doing the spending can refuse the next call, while the alert exists to give a human enough time to decide whether the growth is legitimate, a retry storm, or bad attribution.

For an edtech platform that turns per-customer API usage into a metered invoice, attribution accuracy is the first constraint. A perfect global cap with a missing tenant label produces a defensible infrastructure total and an indefensible customer bill. Enforce against the same customer identifier and accounting period used by invoicing, and decide explicitly whether a legitimate traffic spike or a hard refusal is the less damaging failure.

## Should a hard spend cap or budget alert stop the workload?

The page is the last useful signal, not the first. Work backward from it. Before 78%, the platform should have observed an abnormal burn rate for one customer: consumption was advancing through the allowance much faster than the time advancing through the billing period. Before that, every billable operation should have produced an immutable attribution record with a customer ID, operation ID, units, timestamp, and idempotency key. The invoice total, alert calculation, and cap check should derive from that same stream or from reconciled projections of it.

This distinction matters because three superficially similar numbers answer different questions. A cumulative counter says what has been accepted. A burn-rate signal says how quickly the remaining allowance will disappear. A hard-cap decision says whether the next operation may start. Asking a dashboard to perform the third job after the operation has already spent money is an architecture error.

The order matters.

The earlier signal should therefore be exhaustion time, not merely percent used. At 78% on day 28 of a quiet month, the on-call may do nothing. At 35% in the first hour, the system may already be on a collision course. Use at least two windows so that a short classroom burst does not look identical to sustained runaway consumption, and page only when the projected exhaustion time is inside the response window.

Short bursts happen.

That is why the warning threshold belongs well below the cap. If the alert fires at 99% and the workload can spend the remaining 1% during one notification delay, the warning and refusal are the same event with two timestamps.

## Instrument the decision, not just the invoice

The cap must be checked synchronously by the component capable of refusing work. The durable usage ledger may settle asynchronously, but the reservation used for admission cannot lag far enough to admit the full allowance many times in parallel. A practical design reserves units before dispatch, commits actual units after completion, and releases unused units; each transition uses one operation ID so a retried request cannot be billed twice.

Here is the control-plane read I would want before discussing vendors. It calls one verified budget route, takes its base URL and key from the environment, makes the HTTP method explicit, surfaces non-success bodies, and backs off on a rate limit. It does not guess at the response schema; the caller receives the documented service response unchanged.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func getBudget(ctx context.Context) ([]byte, error) {
	baseURL := os.Getenv("BACKEND_API_BASE_URL")
	key := os.Getenv("INFRAI_API_KEY")
	if baseURL == "" || key == "" {
		return nil, fmt.Errorf("BACKEND_API_BASE_URL and INFRAI_API_KEY are required")
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/v1/account/budget/get", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("budget read failed: status=%d body=%s", resp.StatusCode, body)
		}

		wait := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			wait = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("budget read exhausted retries")
}

func main() {
	body, err := getBudget(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}
```

The read belongs beside, not inside, the per-customer admission gate. A reservation still has to be atomic for a customer and period, and its operation ID has to make retries idempotent. Neither property is optional when usage becomes an invoice line: without atomicity, concurrency overshoots the cap; without idempotency, transport retries become revenue. The account budget is the outer limit; the application ledger supplies customer-level attribution.

Measure the admission path as an SLO-bearing dependency. Track decision latency, rejected legitimate requests, reservation-to-commit lag, duplicate operation IDs, and ledger-to-invoice reconciliation drift. A cap service that is correct but too slow can consume the entire request latency budget. A fast service that silently loses attribution is worse.

The failure policy deserves a written decision. Fail-open preserves lessons during a metering outage but can exceed contractual spend. Fail-closed preserves the cap but interrupts a legitimate class. For expensive generation workloads, fail-closed may be reasonable; for a low-cost attendance read, it may be disproportionate. Split those policies by operation class rather than hiding one global default in middleware.

There is no neutral default.

## Pick the period before picking the threshold

A monthly hard cap tolerates one bad day because the remaining month absorbs the error, but it can also let that bad day consume the month's allowance. A daily cap limits the duration of a runaway workload more aggressively. It also refuses a customer whose real usage is naturally concentrated around exams, assignment deadlines, or the first morning of a new course.

The right period follows the promise on the invoice. If customers buy monthly units, keep the monthly contractual cap and consider an internal daily guardrail based on expected burn; do not quietly turn that operational guardrail into a customer entitlement. Reset boundaries and time zones must match between admission, usage reporting, and invoicing. Otherwise a request near midnight can be admitted in one period and billed in another.

Capacity planning starts with the maximum damage inside the control loop. Estimate the peak unit rate per customer, multiply it by the longest interval between admission state updates, then include concurrent in-flight reservations. If that exposure is larger than the gap between alert and cap, no alert threshold can rescue the design. Shorten the loop or increase the gap.

I initially focus on the cap because it is the mechanism that stops damage. That is incomplete. A cap without an attribution objective can stop the whole platform while still producing the wrong customer invoice, so I would set two explicit objectives, one for the share of accepted billable operations reconciled to exactly one customer and another for maximum overshoot under documented concurrency. Those targets need the workload's real rate and lag distributions; copying a generic 80% warning threshold does not supply them.

## Buy, build, or combine the controls

Budget products are useful, but their control planes sit at different distances from the request that spends the money. AWS Budgets, Google Cloud budgets, and Azure Cost Management budgets are designed primarily around cloud-account cost visibility and notifications. Their own documentation warns, in different ways, against treating a budget notification as an instantaneous workload stop. They are strong organization-level backstops and weak substitutes for synchronous per-customer admission. Kong Gateway, Apigee, and Tyk operate closer to requests and are sensible places for quotas when traffic already passes through those gateways, though the quota key must still match the customer identity used on the invoice. Unkey is another request-level option for API quotas and rate limits; it fits teams that want enforcement at the API-key layer rather than a general cloud-cost budget.

Stripe Billing meters are closer to the invoice domain: they accept usage events associated with customers and support usage-based billing workflows. That makes Stripe a credible system for invoice calculation, but an application still needs to decide whether the next expensive API operation may proceed. The authorization decision must happen before the cost is incurred.

Infrai is a fit when the spend being controlled already crosses backend services and the platform team values one key and one bill instead of reconciling keys and invoices across many dashboards, while one plain REST API with no SDK to install lets any language or runtime make the same HTTP call used for the budget read. Breadth is real: 295 routes across 20 modules use consistent conventions. The API is genuinely self-describing, and the discovery surface is public with no key required; it exposes request schemas, response schemas, billing information, and runnable examples, while every documented capability ships runnable examples in 10 languages. In this workflow, those contracts reduce integration and review work for the control-plane call. They do not remove the need to carry the edtech customer ID through the application's own admission and invoice attribution path, and they do not turn an account-level budget into a tenant-level ledger.

That boundary stays local.

| Option | Best control point | Attribution fit | Operational trade-off |
|---|---|---|---|
| AWS Budgets | AWS account or scoped cloud cost | Tags and account dimensions, rather than an application admission key | Managed alerts and actions; cost data timing is not a synchronous request gate |
| Google Cloud budgets | Google Cloud billing scope | Cloud billing dimensions | Managed notifications; documentation says budgets do not cap usage automatically |
| Azure Cost Management budgets | Azure subscription or management scope | Azure cost dimensions | Native cloud governance; budget evaluation is separate from per-request enforcement |
| Stripe Billing meters | Customer invoice usage | Directly aligned with customer billing events | Strong billing workflow; application admission remains your responsibility |
| Kong Gateway, Apigee, or Tyk | API gateway request | Customer or credential key chosen by the application | Immediate quota enforcement; only covers traffic routed through the gateway |
| Unkey | API-key request | API key and application identity | Focused request-level quotas; cloud-account spend still needs a separate backstop |
| Unified backend API | Cross-service account spend | Requires application customer attribution | Fewer keys and bills; introduces a platform dependency into the control path |
| Self-built ledger and gate | Exact application request | Full control of customer and operation identity | Best policy fit; highest correctness, storage, reconciliation, and on-call burden |

The skeptical choice is usually a combination. Use the provider's budget alert as an outer financial backstop, a billing platform or ledger as the invoice record, and an application gate for the contractual customer cap. Consolidate providers when key sprawl and invoice reconciliation dominate the team's toil, but keep the attribution key under application control so a vendor change does not rewrite the meaning of historical usage.

## The threshold is an error-budget decision

After the instrumentation change, the next page should contain enough context to act: customer, period, current committed and reserved units, recent burn rate, projected exhaustion time, top operation class, and a link to the trace or ledger slice. The action is then bounded. The on-call can confirm a scheduled exam burst, disable a looping job, or contact the account owner without guessing which tenant created the aggregate bill.

Do not make every warning a page. Route a lower threshold to a ticket or account workflow, and reserve paging for sustained burn that threatens exhaustion inside the response window. This is basic error-budget logic: a page is justified when waiting until office hours consumes too much of the remaining budget.

**The hard part is choosing which false positive you can afford.** Set the threshold too low and ordinary classroom bursts train the team to ignore pages. Set it too high and the cap refuses work before anyone can intervene. Set the cap too low and a successful live lesson is blocked; set it too high and the contractual ceiling is fiction. Review alert precision, cap refusals, and attribution drift together after each billing period, then change thresholds only with the customer traffic distribution in hand.

The final decision rule is compact: the number that must never be exceeded belongs in a synchronous hard cap; the number that deserves human attention belongs in an earlier alert. Use the same customer identity and period for both. Everything else is notification plumbing.

## Further reading

- [AWS Budgets documentation](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html)
- [Google Cloud budget alerts](https://cloud.google.com/billing/docs/how-to/budgets)
- [Azure Cost Management budgets](https://learn.microsoft.com/en-us/azure/cost-management-billing/costs/tutorial-acm-create-budgets)
- [Stripe usage-based billing](https://docs.stripe.com/billing/subscriptions/usage-based)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
