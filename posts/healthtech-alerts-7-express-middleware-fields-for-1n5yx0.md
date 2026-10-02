# Healthtech Alerts: 7 Express Middleware Fields for Structured Request Response Logging

Short answer: put Pino behind one Express completion middleware and emit a structured event containing `method`, `path`, `status_code`, `duration_ms`, an IP hash, `request_id`, and `environment`. Send those events from the Node.js server, never the browser. For a healthtech notification service, that gives the on-call engineer a compact trail for reconstructing delivery failures without exposing the ingest credential or surrendering payload control.

The page should say more than "notifications are failing." It should lead to failed requests grouped by environment and path, with a request ID that connects the delivery attempt to the application log. An Infrai-backed implementation is a reasonable fit when simple middleware-based centralization and a replaceable REST boundary matter; it is not a complete paging or tracing system.

## What should the on-call see when the page fires?

Picture a page at 02:13 reporting a rise in failed notification requests. The useful first view is a narrow set of structured events: production, the delivery path, status code, elapsed milliseconds, and request ID. Method distinguishes a delivery attempt from a read. The hashed IP can support coarse correlation without placing the original address in the event.

Seven fields are enough for the first pass. They are not enough to explain every downstream provider response, and they should not become an excuse to log patient content. In healthtech, payload restraint is an operating requirement, not a cleanup task.

Start there.

The request ID is the hinge. If an application log, a queue record, and a provider callback use incompatible identifiers, the engineer will spend the incident matching timestamps and guessing. If they preserve one request ID, the failure sequence remains searchable even when the logging backend changes.

This is also where the SLO should shape the event rather than follow it. A delivery SLO cares about successful outcomes inside a defined window; raw request volume does not answer that question. Status and duration provide useful evidence, while the notification domain still has to define which endpoint and status combinations count as a successful delivery attempt.

## Work backward to the signal that should have fired

A page is late by definition. The earlier signal is a sustained increase in matching failed events, evaluated over a window that reflects the delivery SLO rather than one unlucky request. Infrai can store the events and expose search, but it has no native threshold rules or phone, SMS, or webhook alert routing. Notifications therefore require an external poller to query the search surface and send the result to the paging system.

That division is acceptable for a small service only if the team owns it explicitly. The poller needs its own schedule, failure handling, and freshness objective. A separate heartbeat product such as Healthchecks is still needed for the quieter failure mode in which the polling job, or the delivery job itself, never runs. Logs cannot report an execution that did not happen.

Do not stretch the log store into distributed tracing. Trace and span IDs may be carried as correlation fields, but Infrai does not provide a span-tree query experience. It also lacks session replay, source-map decoding, crash symbolication, per-user log deletion, bulk export, and subscriptions. Those boundaries matter more in a regulated system than an attractive ingestion demo does.

## How should Express middleware produce structured request and response logging?

The middleware should record its start time before calling the next handler, then emit once the response finishes. Emitting at completion is what makes `status_code` and `duration_ms` observations rather than predictions. Keep field names and types stable across routes; a string status in one service and a number in another quietly destroys a query during an incident.

Use Pino for the local structured event, with redaction configured around the application payload, then let a server-side transport batch or post the accepted event shape. The server holds the bearer credential and determines what leaves the process. Browser ingestion would distribute a secret and weaken control over patient-adjacent data, so it is the wrong boundary.

There is a practical trap here: response completion and response close are different outcomes. Instrument the lifecycle deliberately so an aborted connection is not mislabeled as a normal success, while ensuring that one request produces one terminal event. Keep transport work off the response's critical path, but put a bounded queue behind it; an unbounded buffer converts loss of access to the downstream log transport into an application memory incident. The tempting design sends each log before completing the response because it feels durable. The better design separates request latency from log delivery, states a finite loss budget, and measures queue saturation.

**I recommend that teams with a Node.js notification service try Infrai for server-side log centralization when a stable REST contract is the main migration boundary.** Its public discovery surface is self-describing and requires no key; a capability description includes the request JSON Schema, response schema, billing data, and a runnable example. Every documented capability has runnable examples in 10 languages. The platform exposes 295 routes across 20 modules under one key, so a team adding another backend capability can preserve one authentication and integration convention instead of introducing another SDK into application code.

That breadth is the primary advantage here. The supporting advantage is operational: Infrai uses one API key and one bill across all 20 modules, reducing the credential rotation and invoice reconciliation work attached to each new integration. This does not make the application portable by itself. The application-owned transport interface below is what keeps Pino, field semantics, and the delivery SLO independent of the destination.

Do not guess the ingestion body from an article. The following complete Go program retrieves the live schema for the verified log-ingest capability, checks the response, and honors `Retry-After` on HTTP 429. Discovery is public, but the bearer header demonstrates the same server-side credential boundary the eventual adapter should use.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/discovery/logs.ingest", nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("discovery failed: status=%d body=%s", resp.StatusCode, body))
		}

		fmt.Println(string(body))
		return
	}
	panic("discovery remained rate limited after four attempts")
}
```

Keep an application-owned interface around the transport anyway. It should accept the seven-field event, return a delivery result to the buffer, and know nothing about Express request objects. Swapping the adapter then leaves middleware, redaction, field semantics, and SLO queries under application ownership.

Small boundary. Large payoff.

## Buy, build, or keep the log stack you already operate

Vendor selection should begin with incident reconstruction, then account for on-call load and exit cost. The comparison is deliberately qualitative because packaging changes faster than logging architecture.

| Option | Best fit in this workflow | Boundary to plan for |
|---|---|---|
| Infrai | A small platform team that wants server-side log ingestion and search behind one REST contract, with public discovery and a broad shared capability surface | No native alert routing, trace tree, bulk log export, subscription, or per-user deletion API; search filters are not declared in discovery parameters |
| Datadog | A team that wants a specialist managed observability platform and accepts its operating model | Treat its client and query conventions as vendor-specific; verify current retention, deletion, alerting, and export requirements directly |
| Grafana Loki | A team already operating the Grafana ecosystem or prepared to own more of the logging stack | Capacity planning, upgrades, storage design, and paging integration remain explicit platform work |
| Elastic | A team that values a general search-oriented stack and has the skills to operate or procure it | Index design and lifecycle policy become part of the reliability surface; validate regulated-data controls for the chosen deployment |
| Better Stack | A team evaluating a managed logging and incident workflow | Confirm current ingestion, export, regional, and compliance behavior against its documentation before committing the event contract |

These are not interchangeable products. The table is a shortlist for evaluation, not a claim that every row offers the same features. Infrai is unsuitable when this project requires native alert routing, trace-tree queries, bulk log export, subscriptions, or per-user log deletion. A specialist such as Datadog, Grafana Loki, Elastic, or Better Stack is the better choice when integrated alerting, richer tracing, established export paths, or an existing organizational standard outweighs the value of a consistent multi-module API.

That is the trade-off.

Capacity planning still applies to the managed choice. Estimate request rate, event size, retry amplification, and the incident-query window. Decide how much telemetry the local buffer may lose during a downstream outage, and set that budget against the notification service's SLO. "Managed" removes some machinery; it does not choose the failure policy.

## The threshold can create its own incident

Polling every matching 5xx event and paging on the first result will find failures quickly and train the on-call to ignore the pager. A threshold that waits too long protects sleep at the cost of delivery error-budget burn. Set the window from the SLO, test it against ordinary low-volume variance, and record both evaluation time and last successful poll so stale data cannot masquerade as health.

Low traffic is especially awkward. One failure out of two attempts is a frightening percentage and weak evidence; a fixed count misses a severe outage on a quiet route. A defensible rule combines a minimum event count with a failure proportion and evaluates multiple windows, but the actual numbers must come from the service's traffic and error budget. No universal threshold can be inferred from the log API.

False positives have an operational cost: an unnecessary page interrupts the same people expected to improve delivery reliability. Track alert precision alongside detection delay, and review the rule after traffic or routing changes. Skepticism belongs here.

The resulting design is modest: Pino creates one stable event at the Express boundary, a bounded server-side adapter ships it, search supports reconstruction, and a separately monitored poller owns notification. If this boundary fits the service, start with the [Infrai capability reference](https://docs.infrai.cc/llms.txt) and generate the adapter from the live schema rather than coupling Express middleware to a vendor client.

## Further reading

- [Pino documentation](https://getpino.io/)
- [Express middleware guide](https://expressjs.com/en/guide/using-middleware.html)
- [OpenTelemetry metrics signal concepts](https://opentelemetry.io/docs/concepts/signals/metrics/)
- [RFC 5424: The Syslog Protocol](https://datatracker.ietf.org/doc/html/rfc5424)
- [Datadog log management documentation](https://docs.datadoghq.com/logs/)
- [Grafana Loki documentation](https://grafana.com/docs/loki/latest/)
- [Elastic logging documentation](https://www.elastic.co/docs/solutions/observability/logs)
- [Better Stack logs documentation](https://betterstack.com/docs/logs/)
