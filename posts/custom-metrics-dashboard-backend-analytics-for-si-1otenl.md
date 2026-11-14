# Custom Metrics Dashboard: Backend Analytics for Silent Gaming Import Windows

A gaming import can leave its rollout flag enabled while producing no usable results. The operational constraint is proving that a scheduled run delivered output, with enough ownership context to attribute the work. TL;DR: build the product analytics dashboard from backend custom metrics; use feature flags for rollout decisions, and a schedule-aware check for missing results.

Keep the application-facing metrics contract stable when the provider behind it changes. That boundary makes the dashboard decision independent of the rollout system, although it still needs explicit definitions for completion, useful output, and attribution.

## Should a product analytics dashboard use custom metrics or feature flags for import results?

Consider a hypothetical production review: the first question is whether the green card means “the importer is enabled,” “the worker finished,” or “new records became available to the game.” Those are three different claims. This is a design exercise, not an incident report or a measured result.

Suppose a catalog import is due every 15 minutes, with a 5-minute grace period. These are illustrative operating assumptions. One completed attempt reports zero newly usable records; another records 240. The flag value can be identical in both cases. A process exit can also be successful in both cases, so a completion heartbeat alone cannot establish the output condition.

Define success first.

The invariant is that an expected import window needs evidence of usable output by its deadline, attributed to a workload owner. A dashboard chart answers how much output arrived; a deadline check answers whether an expected result is absent. Neither answer follows from flag state. A launch flag can segment the behavior in application code, but the application must emit the resulting conversion, activation, usage, throughput, or error summary as metrics events.

That distinction matters for an SLO. “The scheduler ran” is a different objective from “the scheduled catalog became usable before the deadline.” Choose the latter only for imports whose business contract actually requires fresh output. An unchanged catalog may legitimately yield zero new records; in that case, success needs evidence of a completed validation or a fresh published snapshot, rather than an invented requirement to manufacture changes.

## Attribute work before drawing a cost chart

A chart labeled “import cost” needs a numerator and a denominator with compatible scope. Backend request cost and newly usable game records describe different things, so dividing one by the other requires a documented allocation rule.

Counting enabled flags provides neither.

For this design, define an internal observation with a scheduled window, workload owner, completion state, usable-output count, and a flag cohort captured when the run begins. These are application-owned concepts, not an assertion about any provider's request fields. Keep the individual run identifier in the execution record; putting every run into a metrics dimension can make a growing history into a growing capacity obligation.

A useful first dashboard has output per scheduled window, completed attempts, failed attempts, and attributable backend spend where actual billing evidence exists. Show unknown allocation explicitly. If a shared call serves two game catalogs, assigning all of its cost to whichever catalog finishes first produces a precise-looking chart with the wrong owner.

There is a capacity implication as well: halving the schedule interval doubles the expected windows, before accounting for retries, and attaching every player identifier multiplies the dimension space without improving this particular alert. Plan around workload owners and bounded cohorts. A gaming product dashboard can still track activation and conversion separately; neither belongs in the import liveness denominator merely because the charts share a page.

The main trap is double credit. A retry that republishes the same output must not count as another successful scheduled window. Deduplicate completion evidence at the application boundary, and define what happens when a late attempt finishes after the next window has opened. The dashboard and the alert evaluator need the same window assignment. For example, if west finishes late while east finishes on time, grouping both by arrival time can make the next interval look healthy while the earlier missed deadline disappears from view; preserve the scheduled window in durable evidence, and allow a late completion to update that window's outcome without erasing the original deadline miss. Otherwise attribution and the SLO can disagree even when both calculations consume the same completion record.

## Buy versus build depends on which evidence is missing

Product names don't settle this decision. Start with the question each component can answer, then assign an owner to whatever remains. The comparison below concerns roles in this import design, not a claim that any single product covers its entire lifecycle.

| Option | Useful role | Boundary and buy-versus-build tradeoff |
| --- | --- | --- |
| Prometheus | Collect application metrics and express alert conditions around observed output. | Suitable when the platform team wants to own the metrics model and alert rules; operating the collection and evaluation path adds on-call responsibility. |
| Grafana | Visualize metrics and organize dashboards over configured data sources. | Useful when the team already operates a metrics store; visualization still needs an explicit scheduled-output model and ownership of the underlying data path. |
| Datadog | Combine custom metrics, dashboards, and monitoring in a managed service. | Fits teams choosing managed observability over operating the full stack; instrumentation, dimension choices, and cost allocation remain application responsibilities. |
| Healthchecks | Track expected check-ins from scheduled jobs and detect missing arrivals. | A focused choice for schedule monitoring; send success only after validating the import's output contract, and keep product analytics elsewhere. |
| LaunchDarkly | Manage feature rollout and record which behavior an application should select. | Fits release control; flag enablement alone cannot establish that a particular scheduled import produced results. Maintain explicit completion evidence. |
| Shared capability API | Keep a capability contract stable across changes to its backing vendor, with request-level cost attribution. | Consolidates backend integrations; retain ownership of the import success definition and the schedule-checking component. |
| Application-owned evaluator | Apply a precise deadline and output rule to durable completion records. | Gives control over zero-output semantics and maintenance windows, but the team must operate the evaluator and its notification delivery. |

Infrai provides one key and one bill across 295 routes and 20 modules. A single API key reduces credential handoffs between the importer and dashboard teams; unified billing reduces invoice reconciliation for their backend calls. Its REST API keeps the capability contract stable when the backing vendor changes, while specified per-call cost, vendor, and latency metadata supports request-level attribution. These are useful properties for consolidating backend integrations, provided the team retains the scheduled-output evaluator.

There is a difference between fewer integration surfaces and fewer operational obligations. A stable capability contract reduces the code exposed to a provider change; it does not define the business meaning of an empty catalog import. Public, unauthenticated discovery exposes request and response schemas, billing information, and runnable examples, letting an engineer review the contract without receiving a production key; examples in 10 languages let a Go-based platform team inspect an implementation in its own language. Those are useful handoff properties when dashboard ownership and importer ownership sit with different teams. Request metadata can support attribution, but it doesn't allocate unrelated worker or database work automatically.

Use a dedicated schedule monitor when absence is the problem. Use metrics for the dashboard's quantities. If a flag platform also offers analytics, assess those features on their own documented event semantics; the presence of a flag evaluation still isn't proof of downstream work.

## Put the output condition before the success signal

The preventative path is small: establish the window deadline, persist completion evidence after output is usable, evaluate the expected window, and send a success signal only when its output condition holds. Monitor the evaluator too. Otherwise a silent evaluator can conceal a silent importer.

This runnable Go example keeps the fixture-based output decision separate from a real metrics read, without assuming a vendor response shape or query filter. Save it as main.go and run it with the Go toolchain. Set METRICS_ORIGIN to your configured service's HTTPS origin and INFRAI_API_KEY to its credential. The read prints the response for inspection; it does not turn that response into completion evidence or deliver notifications.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
)

func queryMetrics(ctx context.Context) error {
	origin, err := url.Parse(os.Getenv("METRICS_ORIGIN"))
	if err != nil || origin == nil || origin.Scheme != "https" || origin.Host == "" ||
		origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" ||
		(origin.Path != "" && origin.Path != "/") {
		return fmt.Errorf("METRICS_ORIGIN must be an HTTPS origin")
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}
	origin.Path = "/v1/metrics/query"
	client := &http.Client{
		Timeout: 10 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 2 {
			delay := time.Second * time.Duration(1<<attempt)
			value := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				if requested := time.Duration(seconds) * time.Second; requested > delay {
					delay = requested
				}
			} else if date, err := http.ParseTime(value); err == nil {
				if requested := time.Until(date); requested > delay {
					delay = requested
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("metrics query returned %s: %s", resp.Status, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("metrics query retry budget exhausted")
}

type Window struct {
	Owner    string
	Due      time.Time
	Grace    time.Duration
	Enabled  bool
	Finished bool
	Usable   int
}

func evaluate(now time.Time, w Window) string {
	if now.Before(w.Due.Add(w.Grace)) {
		return "waiting"
	}
	if !w.Finished || w.Usable <= 0 {
		return "alert"
	}
	return "healthy"
}

func main() {
	due := time.Date(2026, time.November, 2, 12, 0, 0, 0, time.UTC)
	now := due.Add(6 * time.Minute)
	windows := []Window{
		{"catalog-west", due, 5 * time.Minute, true, true, 0},
		{"catalog-east", due, 5 * time.Minute, true, true, 240},
		{"catalog-north", due, 5 * time.Minute, true, false, 0},
	}
	for _, w := range windows {
		fmt.Printf("%s enabled=%t usable=%d decision=%s\n",
			w.Owner, w.Enabled, w.Usable, evaluate(now, w))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := queryMetrics(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
```

The enabled field is intentionally visible but absent from the decision. West alerts because its completed attempt yielded no usable output; north alerts because completion evidence is missing. East is healthy under the illustrative output rule. These fixtures demonstrate the decision, not measured production outcomes.

For a deployed evaluator, enumerate expected windows from the schedule independently of completion events. A system that only examines events it received cannot notice a run that never emitted anything. Persist the window identity before execution, record completion after publication, and have a separate process compare the two. The metrics query above is a read, so retrying it does not duplicate a write; any production write path needs its own idempotent completion contract.

## Where the output rule stops applying

Zero output is acceptable for some imports. Encode that explicitly, and distinguish “checked successfully; nothing changed” from “nothing was observed.” Maintenance exclusions belong in the expected-window ledger before evaluation, rather than being inferred from whatever a feature flag happens to say later.

Keep aggregate dashboard metrics separate from user-level investigation. Correlation identifiers are useful evidence, but they do not establish a distributed trace, a replay, or a deletion workflow. If an import handles personal data, assess erasure obligations across its actual storage and telemetry paths; a chart cannot establish compliance with Article 17.

The decision rule is narrow: buy metrics collection and visualization to suit the team's operating capacity, buy or operate a schedule monitor for missing arrivals, and build the application-specific definition of useful output. Allocate spend only where the execution evidence supports an owner. Leave the rest visibly unallocated.

## Sources

References for the alternatives and the privacy boundary:

- Prometheus overview: https://prometheus.io/docs/introduction/overview/
- Prometheus instrumentation practices: https://prometheus.io/docs/practices/instrumentation/
- Grafana dashboards: https://grafana.com/docs/grafana/latest/dashboards/
- Datadog custom metrics: https://docs.datadoghq.com/metrics/custom_metrics/
- Healthchecks documentation: https://healthchecks.io/docs/
- LaunchDarkly feature flags: https://launchdarkly.com/docs/home/flags
- GDPR Article 17, right to erasure: https://gdpr-info.eu/art-17-gdpr/
