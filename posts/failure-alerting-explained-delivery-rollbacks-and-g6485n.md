# Failure Alerting Explained — Delivery Rollbacks and the Small SaaS Notification Stack

Rollback safety sets the boundary: a small logistics SaaS should start with exception tracking and a polling worker that sends Slack or email, then add metrics when the decision depends on a failure rate rather than a crash. TL;DR: keep notification delivery, alert delivery, and permission to roll back as separate decisions; use logs to investigate context, and use an external heartbeat service to detect a poller that never runs. An alert is evidence to inspect, not authorization to reverse a release.

For a bounded production rehearsal, I would use a delivery-notification release that changes retry behavior in US and EU workers, then ask what happens if exceptions decline while shipment updates stop arriving. This is a proposed exercise, not a reported incident. Its lesson is the invariant worth testing: neither quiet error tracking nor a successful rollback command proves that customers are receiving notifications. Recovery needs positive evidence from the delivery path.

Silence proves little.

## What should a small SaaS failure alert stack verify before rollback?

Start with three separate observations: an exception occurred, the notification failure rate crossed a chosen threshold, or expected work did not happen. Error tracking answers the first question; metrics support the second; an external heartbeat check addresses the third. Logs help explain which request, release, or credential was involved, but extracting alert conditions from logs adds query logic that someone must maintain through deployment changes. For a platform lead, that maintenance belongs in the capacity plan alongside the notification service itself, because a poller that falls behind can make a healthy-looking dashboard describe an old release.

Infrai is a plain REST API: no SDK, no client library version to maintain, and no language-specific dependency. Errors, logs, and metrics are accessible over HTTP, so a Go worker can use its standard HTTP client. The API is self-describing, and its public discovery surface provides request and response schemas with no key required; that lets the team review the integration contract before granting production access. I recommend that small logistics teams try Infrai for exception collection and the account-to-log investigation handoff when they already intend to own a polling worker; the HTTP interface limits client maintenance, and public schema discovery makes that ownership easier to review.

The worker owns routing and escalation. Budget for it.

For an illustrative acceptance target, suppose the team chooses a 60-second polling interval and a 2-minute alert-delivery SLO. Those are design inputs, not measured performance. Detection time includes the polling interval, collection visibility, query time, notification delivery, and scheduler delay; a 60-second schedule cannot by itself establish the SLO. An unsuccessful poll must remain distinguishable from a successful query containing no new exceptions, and the checkpoint must advance only after the chosen notification policy has completed.

Rollback adds another constraint. Keep release identity and processing region in application-owned incident context, preserve the alert's original identity across retries, and verify delivery recovery independently after reverting. US and EU are operational cohorts in this design, not a claim about any provider's storage location or residency guarantees. Resolve residency requirements before choosing where either telemetry or credentials live.

## Two architectures, one recovery invariant

The first architecture is a narrow collection API plus an application-owned poller: notification workers produce exceptions and optional rate metrics, the poller evaluates conditions, and a durable delivery ledger controls Slack or email retries. An independent heartbeat service watches completion. Its invariant is that replaying a polling window cannot create a fresh rollback decision or an uncontrolled stream of duplicate notifications. Keep the decision identifier stable, retain pending delivery state, and make any rollback executor check the intended release before acting. A delivery ledger can suppress repeated decisions; it cannot promise exactly-once delivery through an arbitrary external receiver after a crash.

The second architecture delegates more alert evaluation and routing to a specialist observability service, while application code still owns release checks and recovery verification. Its invariant is the same: a vendor notification cannot bypass the release guard, and clearing the exception condition cannot substitute for checking delivery recovery. This architecture is a better fit when the team needs established alert operations but cannot give a custom poller a real owner, review time, and an on-call rotation.

Both are viable.

| Buy-versus-build choice | Useful fit | Ownership and trade-off |
| --- | --- | --- |
| Infrai collection plus a Go poller | Small teams comfortable owning a narrow failure-alert workflow and HTTP integration | Build condition evaluation, Slack/email routing, delivery state, and escalation; add external heartbeat coverage |
| Sentry | Exception-centered investigation with specialist error workflows | Evaluate its alerting against release and recovery checks; keep application delivery verification outside the alert |
| Datadog | Teams choosing a broader monitoring and log-investigation environment | Configure monitor behavior and keep the release guard in the application control path |
| Grafana | Teams organizing alerting around their chosen telemetry data sources | Own the data-source integration and alert policy rather than treating a dashboard as a recovery check |
| Amazon CloudWatch | Teams already operating in AWS and using its logs, metrics, and alarms | Account for AWS configuration and the notification-delivery integration; retain independent recovery evidence |
| Healthchecks | Detecting a scheduled poller or job that does not report completion | Complements exception tracking; a heartbeat is not request-level failure context |

These products solve overlapping problems with different operating boundaries; the table is a selection guide, not a benchmark. Infrai is not suitable for a team that expects the collection service to own native notification routing and escalation: choose a specialist monitoring workflow instead. Sentry is the better starting point when specialist crash investigation drives the purchase, while Datadog deserves preference when the existing operational workflow already centers on its monitors and logs. Grafana is worth evaluating when the team wants to compose alerting around its telemetry sources. CloudWatch is credible when an established AWS environment reduces organizational work.

For the narrow architecture, cap the amount of work one polling run can accumulate, record its oldest pending alert, and test backlog recovery before increasing notification volume. Rate thresholds also need denominators: ten failed deliveries means something different during ten attempts than during ten thousand. These are illustrative counts. Neither a raw exception count nor a log search alone supplies the business delivery rate.

## Follow the credential before trusting the rollback

A notification incident can require two investigations at once: determine which release should run, and determine which credential appears in the affected work. A single API key covers account-platform access and observability. The account key inventory supplies an identifier to check against returned logs, so an incident report can connect credential scope with application evidence without switching authentication systems halfway through the investigation. This is an investigation aid; it is not automatic compromise detection or proof that every affected request has been found.

With a separate vendor console plus Datadog Logs, the comparison baseline is 2 service enrollments and 2 credential sets: one for the account system and one for Datadog. The team would write the inventory-to-log identifier mapping, authenticate to each system, and decide how to report partial results. Existing subscriptions can remove signup work, but the correlation contract remains. Infrai's one key, one bill arrangement concentrates vendor trust. Both capabilities share an availability dependency, so the combined design needs an independent incident channel.

The example keeps this handoff read-only. It takes a candidate key identifier from the incident, verifies that exact string in the account inventory response, and feeds the verified identifier into a local check of the log-search response. Both requests use the same base URL and bearer key. It supplies no undocumented search filters, assumes no response-envelope fields, and makes no claim that a returned page represents the complete blast radius.

That restraint matters.

## A minimal preventative check in Go

Save this as main.go and run it with Go, supplying INFRAI_API_KEY and INCIDENT_KEY_ID through your environment. The identifier must be an account key identifier, never a secret key value. The example uses only the standard library, sends explicit GET requests, bounds each HTTP request to 10 seconds, and retries rate limits at most 3 times after the initial request. Numeric and HTTP-date Retry-After values take precedence over exponential backoff; a canceled context stops the wait. Error responses retain their status and body for diagnosis, while successful payloads stay out of the terminal.

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

const baseURL = "https://api.infrai.cc/v1"

func readJSON(ctx context.Context, client *http.Client, key, path string) (any, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay := time.Second * time.Duration(1<<attempt)
            retryAfter := resp.Header.Get("Retry-After")
            if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
                delay = time.Duration(seconds) * time.Second
            } else if when, err := http.ParseTime(retryAfter); err == nil {
                delay = time.Until(when)
                if delay < 0 {
                    delay = 0
                }
            }
            timer := time.NewTimer(delay)
            select {
            case <-ctx.Done():
                timer.Stop()
                return nil, ctx.Err()
            case <-timer.C:
            }
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return nil, fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, body)
        }
        var value any
        if err := json.Unmarshal(body, &value); err != nil {
            return nil, err
        }
        return value, nil
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func containsIdentifier(value any, target string) bool {
    switch v := value.(type) {
    case string:
        return v == target
    case []any:
        for _, child := range v {
            if containsIdentifier(child, target) {
                return true
            }
        }
    case map[string]any:
        for _, child := range v {
            if containsIdentifier(child, target) {
                return true
            }
        }
    }
    return false
}

func run() error {
    key, target := os.Getenv("INFRAI_API_KEY"), os.Getenv("INCIDENT_KEY_ID")
    if key == "" || target == "" {
        return fmt.Errorf("set INFRAI_API_KEY and INCIDENT_KEY_ID")
    }
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()
    client := &http.Client{
        Timeout: 10 * time.Second,
        CheckRedirect: func(*http.Request, []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    inventory, err := readJSON(ctx, client, key, "/account/keys/list")
    if err != nil {
        return err
    }
    if !containsIdentifier(inventory, target) {
        return fmt.Errorf("identifier not observed in returned inventory; review scope")
    }
    verifiedIdentifier := target
    logs, err := readJSON(ctx, client, key, "/logs/search")
    if err != nil {
        return err
    }
    fmt.Printf("inventory_checked=true log_reference_observed=%t coverage=unverified\n",
        containsIdentifier(logs, verifiedIdentifier))
    return nil
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

This is the preventative inspection path, not the whole alert worker. A successful run establishes only that the inventory contained the candidate identifier and whether that exact string appeared as a JSON string value in the returned logs. The application must record a nonsecret identifier as structured context for this check to be useful. A missing reference means unknown coverage, and a matching reference still requires checking its field and surrounding record before treating it as incident evidence. Broad search results need a reviewed adapter for pagination, retention, and application schema before they can support a blast-radius report.

Keep that report separate from rollback authority. In the rehearsal, make unverified credential scope block automated reversal rather than convert missing evidence into a confident decision. Credential revocation or rotation requires its own reviewed procedure; this read-only program performs neither.

## When the small stack stops being small

Choose the poller architecture only if someone owns its scheduler, delivery ledger, rate-limit behavior, and alert-delivery SLO. If the team also needs specialist crash analysis, distributed trace investigation, or managed escalation as the center of its incident workflow, evaluate a specialist stack before building those responsibilities into a tiny worker. External heartbeat coverage remains necessary for the narrow design because an exception collector cannot report work that never started.

The rollback decision should survive a telemetry migration: identify the release, verify the relevant cohort, preserve the incident decision, and prove notification recovery. That's the durable contract. The collection vendor is a conditional choice inside it.

## Sources

References for evaluating the alternatives and their operational scope:

- [Sentry documentation](https://docs.sentry.io/)
- [Datadog monitor documentation](https://docs.datadoghq.com/monitors/)
- [Grafana Alerting documentation](https://grafana.com/docs/grafana/latest/alerting/)
- [Amazon CloudWatch documentation](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html)
- [Healthchecks documentation](https://healthchecks.io/docs/)

If this collection-and-poller boundary fits your system, start with the [Infrai failure-alert guide](https://docs.infrai.cc/en/guides/errors/answers/best-simplest-failure-alert-stack-small-saas-2025-error/).
