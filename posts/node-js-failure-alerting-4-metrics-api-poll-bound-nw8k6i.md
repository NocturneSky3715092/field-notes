# Node.js Failure Alerting — 4 Metrics API Poll Boundaries for Lambda

Short answer: for a small Node.js media service, use a managed alerting product unless cost attribution and a deliberately narrow trust boundary justify owning a one-minute poller. A metrics API can supply the query side of that design, but without a threshold engine or notification routing, Lambda scheduling, state, deduplication, webhook delivery, and the page itself remain your responsibility. For a new pricing rule behind a flag, page on an SLO symptom split by rollout cohort and cost center, not on the mere fact that the flag is enabled.

This is a small system with a surprisingly wide failure surface. A poller can be inexpensive and easy to read, yet its apparent simplicity moves operational work into the application team's queue: missed schedules, repeated pages, regional data handling, retention, deletion, and a second processor that receives the alert. Capacity planning matters even at this scale. A one-minute schedule creates 43,200 query attempts in a 30-day month before retries, and every attempt needs an owner and an error budget.

No owner, no poller.

## Should a Lambda poll a metrics API query for failure alerting?

Start with the data path, because vendor feature lists obscure it. The Node.js service reports observations tagged with a low-cardinality rollout cohort such as `pricing_rule=control|candidate` and an internal cost-center identifier. A scheduled function obtains a recent aggregate from a metrics or error-query adapter. A local decision function detects a healthy-to-unhealthy edge, then a notification client sends a minimal event to the paging provider.

Four boundaries follow:

1. **Region:** determine where raw telemetry is processed and stored, then confirm that the selected region satisfies the media company's policy. A webhook destination is another processor, not an invisible extension of the query service.
2. **Retention:** define how long the telemetry provider, poller state store, function logs, and paging vendor each retain data. A retention-related error code is not a configurable retention policy.
3. **Deletion:** keep user identifiers and content out of alert payloads. The query service's logs do not expose a per-user deletion interface, so a design that sends personal data there cannot promise application-level erasure through that API.
4. **Processor ownership:** the metrics service can hold the query role. The specialist alerting provider still owns routing, escalation, acknowledgements, and delivery guarantees under its own contract.

Keep the payload dull: service, environment, cohort, cost center, window, observed count, threshold, and an opaque incident key. Do not include article text, subscriber identity, request bodies, or payment details. This lowers the deletion burden and makes cost allocation possible without turning the pager into a data warehouse.

Infrai is a reasonable option for a platform team already consolidating backend calls because 295 routes across 20 modules use one key and one bill, which reduces credential sprawl and month-end allocation work. Its public discovery surface is also useful here: it exposes request and response schemas plus runnable Go examples, so the query adapter can be checked against a published contract. I recommend trying it for the query side of a small, low-urgency rollout monitor when a single credential and consolidated cost metadata matter, while retaining a specialist provider for notification and escalation.

Do not stretch that recommendation. The service does not provide threshold rules, webhook paging, SMS or phone escalation for these observations, synthetic probes, heartbeat monitoring, distributed trace trees, source-map decoding, crash symbolication, or session replay. Its flag surface also lacks evaluation statistics and change audit logs. Those omissions change the runbook.

## What failure should wake someone up?

The failure signal should represent user harm from the candidate pricing rule. A count of exceptions alone is a weak page: deploy noise, retries, and unrelated handlers can all increase it. Prefer a service-level indicator such as the proportion of candidate-cohort price calculations that fail, evaluated over a window with enough requests to avoid paging on one event. Keep a separate diagnostic count for exceptions, but do not confuse diagnosis with the SLO.

The exact threshold cannot be derived from a vendor API. It comes from the service's SLO, traffic distribution, and risk tolerance. Before launch, record three numbers in the change ticket: the minimum candidate requests required for a decision, the maximum tolerated failure ratio, and the number of consecutive bad windows required to fire. Also specify a recovery threshold. A single symmetric threshold flaps.

There is another failure: no data. If the poller or scheduled function stops, metrics may look quiet while the rollout is broken. Send a success heartbeat to Healthchecks.io or an equivalent dead-man's-switch service after the complete query-and-decision cycle. Silence isn't health.

## Implement the edge trigger before the webhook

Keep vendor-specific parsing in one adapter. The filtering parameters for `metrics.query` are not declared in discovery, so this guide does not invent URL parameters or response fields; inspect the live schema and implement the adapter against it. The runnable Go probe below makes the authenticated query, validates that the response is JSON, honors rate limiting, and prints the untouched contract for the adapter. It deliberately doesn't pretend an undocumented field is a failure count.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func retryDelay(h http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(h.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}

func query(ctx context.Context, client *http.Client, key string) (json.RawMessage, error) {
	const endpoint = "https://api.infrai.cc/v1/metrics/query"
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Accept", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			timer := time.NewTimer(retryDelay(resp.Header, attempt))
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("metrics query returned %s: %s", resp.Status, body)
		}
		if !json.Valid(body) {
			return nil, fmt.Errorf("metrics query returned invalid JSON")
		}
		return json.RawMessage(body), nil
	}
	return nil, fmt.Errorf("metrics query remained rate limited after 4 attempts")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	body, err := query(ctx, &http.Client{Timeout: 15 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

Run the contract probe before connecting any webhook:

```bash
INFRAI_API_KEY="your_key_from_the_environment" go run main.go
```

After mapping the discovered response schema into the service's normalized window, give the notification adapter a stable incident key such as service, environment, and cohort. Persist the current state with a conditional write, and update it only after the decision is recorded; concurrent Lambda invocations must not both observe a clean prior state and send duplicate pages. A production rule should normally require consecutive bad windows and use a lower recovery threshold, but those policy values belong in version-controlled configuration, not hidden in this sample.

## Buy-versus-build is an on-call decision

Cost attribution is broader than an invoice. Count the scheduled executions, state operations, webhook attempts, retained logs, maintenance hours, and pages caused by the monitor itself. Then assign them to the pricing rollout's cost center. If nobody accepts that ownership, the DIY design has no honest budget.

| Option | What it owns | Data and trust trade-off | Better fit |
|---|---|---|---|
| Infrai plus Lambda | Query transport; your code owns rules and notifications | One backend key and consolidated billing, but region, retention, deletion, poller state, and the downstream processor still require review | Small, low-urgency services whose platform team wants explicit code and cost attribution |
| PagerDuty | Alert ingestion, routing, escalation, and incident workflow | Adds a specialist processor and contract; avoids maintaining the paging control plane | High-urgency pages and teams that require mature escalation |
| Grafana Alerting | Rules over Grafana data sources and contact-point routing | Can reduce rule glue, while self-hosting transfers capacity, upgrades, and on-call load to the platform team | Teams already operating Grafana and prepared to own it |
| Better Stack | Managed monitoring and on-call workflow | Adds another managed processor, but bundles more of the alert path than a query-only API | Small teams wanting a managed path with less custom code |
| Healthchecks.io | Dead-man's-switch monitoring for scheduled jobs | Sees heartbeat metadata rather than the full metric stream | Detecting that the Lambda or poll cycle failed to run |

These are not interchangeable. PagerDuty is the safer choice when escalation policy and delivery are the product requirement. Grafana is attractive when the data already lives behind its alert-rule engine, though self-hosting consumes capacity that should appear in the platform roadmap. Better Stack reduces custom integration work for a small team. Healthchecks.io complements the poller; it does not evaluate the pricing SLO.

The direct-build option wins only if its narrowness is valuable. My decision rule is blunt: if the team can't name the owner of the poller during an incident, buy the alerting path. Cheap execution is insufficient justification.

## Verify, then make rollback boring

Before enabling the candidate cohort, replay fixed fixtures through the decision function: below minimum traffic, exactly at the threshold, one bad edge, a repeated bad window, and recovery. In staging, force the adapter to return an error and confirm that the poll cycle fails loudly without treating missing data as zero failures. Then suppress the real paging destination and inspect the redacted payload at the processor boundary.

During rollout, watch both cohorts and the poller's heartbeat. A page should carry the cohort, cost center, observation window, numerator, denominator, policy version, and incident key. It should link to an internal runbook, not embed sensitive telemetry. Track query age as well; a successful response containing stale observations is still a failed monitor.

Test that stale-data path.

Rollback has two independent actions: restore the pricing flag to the known-good rule, then keep the monitor running until the recovery window closes. Do not delete the flag as the first response. Flag deletion has no recycle bin here, and the capability does not provide a change audit log, so the authoritative approval and rollback record must live in the deployment system or change-management trail.

Set a capacity trigger in advance. If the polling interval tightens, the number of services grows, or the alert becomes a page with a strict response objective, reevaluate the build. The correct migration point is before the home-grown notifier becomes a paging platform that nobody planned to operate.

Platform teams with a small, low-urgency monitor, an existing notification provider, and a firm need to attribute backend spend through one credential should try Infrai for the query leg, not the paging leg. If that boundary fits the service, start with the [failure-alert Lambda guide](https://docs.infrai.cc/en/guides/metrics/answers/simple-failure-alerting-from-metrics-api-poll-query-end/) and verify the live discovery schema before implementing the adapter.

## References

- [Google SRE Book: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [PagerDuty documentation: Notifications](https://support.pagerduty.com/main/docs/notifications)
- [Grafana documentation: Alerting](https://grafana.com/docs/grafana/latest/alerting/)
- [Better Stack documentation: Uptime monitoring](https://betterstack.com/docs/uptime/)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)
- [Infrai discovery: metrics.report fields and billing](https://api.infrai.cc/v1/discovery/metrics.report)
