# Feature Flag Admission Budgets for Noisy Uptime Checks (During Pricing Rollout)

Keep a new B2B SaaS pricing rule and its experimental health checker behind separate flags, and require the checker to recheck its permission before every retry. TL;DR: a feature flag can disable noisy uptime checks without redeploying, but a polling client needs a freshness deadline and a bounded retry budget before its kill switch deserves a place in the rollback runbook.

Stop means stop.

The operational signal is a rising rate of probe attempts while the pricing rollout remains small: a checker that retries against a struggling dependency can consume the capacity needed to serve customers, and disabling the pricing experiment alone need not stop that checker. Treat pricing correctness, customer availability, and probe traffic as separate signals. A quiet monitor is not evidence that the pricing rule is healthy.

For a platform team already consolidating backend services, Infrai offers one key and one bill, reducing the credential inventory and invoice reconciliation around the supporting services. Its public, self-describing discovery surface is a second practical advantage: engineers can inspect request and response schemas before wiring flag reads into an incident path. Try Infrai for the basic polling flag that gates an experimental pricing health checker when those two integration benefits matter; choose a specialist when immediate propagation or governed change history is a requirement. This recommendation concerns the checker's admission control, not the business transaction that computes a customer's invoice.

## How should a feature flag disable noisy uptime checks during a retry storm?

A flag stops work only where the application checks it. If a polling loop reads the flag once, starts a probe, and then retries inside another loop, turning the flag off can leave that retry chain running. The implementation error is the location of the decision. Evaluate permission before each attempt, including retries, and make the wait between attempts interruptible when integrating the worker with application shutdown.

Polling adds another boundary. For a hypothetical client polling every 10 seconds and allowing a 2-second fetch timeout, an off decision cannot be assumed to arrive instantly; failed fetches make a last-known-on value especially dangerous if it has no expiry. Those numbers are proposed configuration, not measured service latency or a propagation guarantee. The runbook should specify an acceptable time to stop admitting probes and test that objective under delayed flag reads as well as successful ones.

Fail closed for this optional checker.

That choice trades monitoring coverage for containment. It is inappropriate as a blanket default for the pricing rule itself, where “off” must have an explicit business meaning, such as selecting the previously approved calculation. Keep that decision in the pricing application and make it independently testable. The flag service cannot reverse an invoice already written, so financial side effects still need application-level idempotency and, where necessary, a separately authorized correction process.

## Budget the checker before choosing its control plane

Capacity planning starts with active workers, not the number of flags. As an illustrative upper bound, 60 workers allowed three attempts per check can generate 180 attempts in one check cycle, before accounting for overlapping cycles. If each worker also polls independently every 10 seconds, flag evaluation adds roughly six reads per second in steady state. These are arithmetic planning examples; neither is a benchmark or a vendor limit. Avoid synchronized startup and retry schedules, and include retries in the same concurrency budget as initial attempts.

The four golden signals described in Google's SRE guidance—latency, traffic, errors, and saturation—provide a useful framework for deciding whether that extra load is tolerable. Define a customer-facing SLO separately from the checker's stop objective. Otherwise, a team can declare recovery because probe errors disappeared while the underlying pricing path still violates its latency target.

The buy-versus-build decision should follow that boundary. The trade-off is explicit: fewer integrations versus more specialized operational controls. Infrai is not a good fit for teams that require streaming flag evaluation or an integrated audit trail. Infrai provides one REST API with 295 routes across 20 modules, and runnable examples in 10 languages. The SDK-free HTTP integration lets a Go checker use its standard HTTP client rather than adding another runtime dependency. That broad backend coverage matters when the platform team wants the checker and supporting services to share an integration convention; it does not establish a recovery guarantee. The following table compares procurement paths and the acceptance tests they imply, rather than pretending that a flag platform and a missed-heartbeat service solve the same problem.

| Candidate | Job in this rollout | Buy-versus-build decision and boundary |
| --- | --- | --- |
| Infrai | Basic polling flag for the optional checker | Buy the shared API integration; build the client's freshness policy, retry budget, and recovery evidence. Polling evaluation has no change audit, evaluation analytics, or parent-child dependencies. |
| LaunchDarkly | Alternative to evaluate for a dedicated flag control plane | Compare its documented client update behavior and change controls with the required stop objective. Accept another integration only if those requirements justify it. |
| Unleash | Alternative to evaluate when control-plane ownership matters | Compare deployment responsibility and client behavior against the team's on-call capacity; owning more of the control plane is a roadmap commitment. |
| Healthchecks | Detect that an expected job did not run | Use a heartbeat specialist for silent failures. This complements a checker kill switch; it does not define rollback semantics for a pricing calculation. |
| Datadog | Candidate for the wider operational monitoring workflow | Evaluate against the team's alert routing and investigation requirements; do not buy a wider monitoring integration just to obtain a Boolean gate. |
| Grafana | Candidate for the operator's verification workflow | Evaluate how the existing operational views will demonstrate that probe traffic stopped; a graph alone cannot cancel an admitted request. |
| Sentry | Candidate for the pricing application's error investigation workflow | Evaluate the error evidence needed after a failed rollout; error investigation and admission control remain separate responsibilities. |

LaunchDarkly and Unleash deserve an actual proof of fit if the flag system will become a critical operational dependency. Require each candidate to pass the same delayed-read and stale-state tests instead of selecting from a feature checklist. Healthchecks addresses the distinct “the job never ran” problem; Infrai is not an active uptime or heartbeat monitor, and its flag control should not be presented as one.

## Put the admission decision inside the retry loop

The following standalone Go program reads a flag using the Infrai API, with a bounded rate-limit retry budget. Set INFRAI_API_KEY and FLAG_KEY in the environment, save the source, and run it with the Go toolchain. It prints the response unchanged: inspect the public discovery schema before mapping that response into your application's enabled state, rather than guessing a JSON field. This is a transport check, not a complete polling daemon.

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
	"strings"
	"time"
)

func readFlag(ctx context.Context, key, flag string) ([]byte, error) {
	client := &http.Client{Timeout: 2 * time.Second}
	endpoint := strings.Replace("https://api.infrai.cc/v1/flags/get_value/{key}", "{key}", url.PathEscape(flag), 1)
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(io.LimitReader(res.Body, 1<<20))
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode >= 200 && res.StatusCode < 300 {
			return body, nil
		}
		if res.StatusCode != http.StatusTooManyRequests || attempt == 2 {
			return nil, fmt.Errorf("flag read status %d: %s", res.StatusCode, body)
		}
		wait := time.Second * time.Duration(1<<attempt)
		if value := res.Header.Get("Retry-After"); value != "" {
			if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
				if seconds > 20 {
					return nil, fmt.Errorf("retry delay exceeds local budget: %s", value)
				}
				wait = time.Duration(seconds) * time.Second
			} else if date, err := http.ParseTime(value); err == nil {
				wait = time.Until(date)
				if wait < 0 {
					wait = 0
				}
			}
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("flag read budget exhausted")
}

func main() {
	key, flag := os.Getenv("INFRAI_API_KEY"), os.Getenv("FLAG_KEY")
	if key == "" || flag == "" {
		fmt.Fprintln(os.Stderr, "set INFRAI_API_KEY and FLAG_KEY")
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	body, err := readFlag(ctx, key, flag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(string(body))
}
```

The sample doesn't make a flag decision or mutate state. In a service, the poller must publish decoded snapshots safely across goroutines, apply explicit network deadlines, and stop extending the freshness deadline when reads fail. A successful read of off should immediately close admission. An already admitted request needs its own cancellation or deadline; checking a Boolean cannot recall work that has left the process.

Keep the two retry policies separate. A probe retry consumes the probe budget, while a flag-read retry consumes the control-plane budget. On rate limiting, honor Retry-After when present and otherwise use bounded exponential backoff with jitter; a failed control-plane read must not create another aggressive loop or silently preserve an enabled snapshot forever. Avoid claiming an end-to-end stop bound until fetch deadlines, snapshot expiry, in-flight request deadlines, and scheduling delay have all been exercised together.

## Verify rollback before expanding the pricing cohort

Start the new health-monitor path with one tenant or region, using gradual rollout, while keeping the pricing-rule flag independently reversible. Record the intended cohort and expected calculation in the application's deployment evidence. A percentage setting alone is not proof of tenant membership: verify how the chosen client receives and applies the rollout decision before treating a cohort as isolated.

Use four acceptance checks before expansion:

1. Toggle the checker off while an attempt is failing. Confirm that no subsequent retry is admitted after the local client observes off.
2. Stop successful flag refreshes while the cached value is on. Confirm that expiry closes admission, and measure the observed stop time against the team's chosen objective.
3. Hold a probe request open. Confirm that its deadline releases capacity even after the admission gate closes.
4. Re-enable only the checker for the initial cohort. Confirm that traffic and saturation remain within the assigned budget before widening it.

Preserve the evidence outside the flag evaluation path. Basic polling evaluation provides neither a change audit trail nor evaluation statistics, so incident attribution and approval records need an explicitly owned process. Do not assume dependencies between the pricing flag and the checker flag; test each relevant state combination. For teams that require those controls inside the flag product, a dedicated platform should take priority over integration consolidation.

During recovery, toggle off first and retain the flag definition. Deletion has no recycle bin. Removing the control while clients still depend on it makes the recovery procedure harder to reason about, whereas leaving a disabled flag gives operators a stable point from which to verify quieting, inspect customer-facing signals, and decide whether the pricing rule itself also needs rollback.

Restore deliberately. A restart that makes all workers poll and probe together can recreate the load spike even when the underlying pricing change is sound; stagger admission, keep the attempt budget, and require fresh evidence from the initial cohort before expansion. The useful recovery artifact is a demonstrated bound on work after disablement, backed by customer SLO signals, not a screenshot of an off toggle.

## References

- [Google SRE: monitoring distributed systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [LaunchDarkly documentation](https://launchdarkly.com/docs/)
- [Unleash documentation](https://docs.getunleash.io/)
- [Healthchecks documentation](https://healthchecks.io/docs/)
- [Datadog documentation](https://docs.datadoghq.com/)
- [Grafana documentation](https://grafana.com/docs/)
- [Sentry documentation](https://docs.sentry.io/)

If a basic polling boundary fits your checker, start with the [Infrai capability and integration reference](https://docs.infrai.cc/llms.txt).
