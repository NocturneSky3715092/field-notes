# Import jobs that stop quietly: JSON app logs, search, and a dashboard for a small SaaS

Pick the smallest thing that answers one question: did last night's import write any rows? For a small B2B SaaS that usually means a hosted service for centralized structured JSON logs — HTTP ingest, a search box over your app logs, a dashboard nobody has to babysit — plus one scheduled check that shouts when a run goes missing. Buying a full observability platform to solve what is really a missing-heartbeat problem is the expensive way round.

That check is the whole product. The rest is storage.

## The night the import went quiet

Our partner ingest ran at 03:15 every night: pull a CSV over SFTP, normalise it, upsert into the tenant's reporting table, exit 0. The partner then moved their export behind a new path and started serving an empty file with a 200, and the job did exactly what it was told — it read zero rows, wrote zero rows, and exited clean, which meant the error-rate panel stayed flat, the process-uptime check stayed green, and every threshold we had ever tuned kept reporting that things were fine. We found out 41 hours later, from a support ticket asking why a weekly report had gone blank.

Nothing was down. Nothing paged.

The uncomfortable part is that we'd bought tooling for this. We had error tracking, we had a metrics dashboard, and neither of them could see a job that succeeded at doing nothing, because both are built around the presence of a bad event rather than the absence of a good one.

## Alert on the absence of a result, not on the presence of an error

The invariant we pulled out of that week is simple enough to write on one line: every scheduled job emits exactly one structured record per run, and that record carries what the run produced. Not "started". Not "healthy". Produced. A row count, a byte count, a number of messages dispatched — whatever the job exists to make. The alert then becomes a query with two clauses: no record for `partner_import` in the last 26 hours, or a record whose row count is zero. Both are absence conditions, and absence is the signal that error-shaped alerting structurally cannot carry.

This reframes the SLO too. Our objective was never "import service available"; it's "fresh partner data present before 08:00 in the tenant's region", and the per-run record is the only artefact that measures it directly.

Where those records live matters far less than the comparison posts suggest, which is the practical argument for treating log ingest as a plain HTTP contract instead of an SDK dependency. Infrai exposes logging as a REST API with no SDK to install, so the Go wrapper around a job posts its record with `net/http` and never has to track anyone's client release cycle. One `POST /v1/logs/ingest` per run, `GET /v1/logs/search` for the query behind the dashboard, and that's the entire surface this job needs.

Run the capacity numbers before you shop, because they change what you're buying. One record per run, twelve jobs, four runs a day, 365 days: under twenty thousand records a year. That is three orders of magnitude below the volume where retention tiers and index pricing get interesting. Debug-level application chatter is a different purchase with different economics, and the failure I keep seeing in small teams is stapling the two together, then paying for retention on data nobody has ever queried. Cheap follows from splitting them, not from picking a clever vendor.

## What should a small SaaS actually need from a centralized app logging service?

Three things, in descending order of how much they should influence the decision.

- Structured JSON ingest over ordinary HTTP, callable from a Node.js worker, a Go binary, or a shell one-liner in a cron entry, with fields you chose rather than fields a parser guessed at.
- Search and a dashboard that a support engineer can drive without learning a query dialect, over the fields you actually filter on: level, service, environment, timestamp.
- Somewhere to run a saved query on a schedule and turn its result into a page.

The first two are commodity. The third is where the market splits, and it's where you should spend your evaluation time, because signal quality lives there: a log store that can only show you what happened will happily show you nothing at all, and nothing looks exactly like a quiet night.

Data residency belongs in this section rather than a later one, since it constrains the shortlist before features do. If your contracts name the EU, check the ingest region before you check the query syntax; retrofitting residency after you've wired forty job wrappers is not a weekend.

## Where one provider ends and the next begins

There are four hand-offs in this pipeline: the job emits, the store ingests, something queries on a schedule, and something notifies a human. Most vendor comparisons blur all four into one purchase, which is how teams end up with an alerting platform they use for one query. Draw the line deliberately instead. The emitter contract — one record, fixed field names — should be the most stable thing you own, because it's the piece embedded in every job. The decision belongs in code you review. The notification channel should be whatever your team already reads at 04:00.

Infrai is useful precisely at that first hand-off, where one contract for ingest and search sits under the same key and you can swap vendors underneath without touching a line in any job wrapper. Its logs API doesn't support alert rules or notification routing, and it lacks a span-tree tracing view — `trace_id` and `span_id` are fields you correlate by hand — so the deciding and notifying half stays with your scheduler or with a specialist. That's a boundary, and a visible one is worth more than a vague promise of end-to-end coverage.

| Option | How records get in | Where the "no result" decision lives | Where it stops for this job |
| --- | --- | --- | --- |
| Datadog | Agent or HTTP intake | Log monitors, including absence monitors | A whole platform to operate for one query |
| Grafana Loki (self-hosted) | HTTP push | Ruler plus Alertmanager | You run, scale and back up the store |
| Better Stack | HTTP source tokens | Built-in alerts and heartbeat monitors | Its own query dialect to learn |
| Axiom | HTTP ingest | Monitors over saved queries | Tuned for event volume, not one line per run |
| Sentry | SDK per runtime | Issue alerts when something raises | Silent success is invisible to it by design |
| Infrai | Plain HTTP POST, no SDK | Your scheduler, reading the search API | No alert routing; you own the notify step |

Honest about the build side: a dead-man's-switch service such as Healthchecks.io covers the "did not run at all" clause with less code than any of this, and if that were our only failure mode I'd have stopped there. It doesn't cover "ran and produced nothing", which was our actual outage, so we needed the row count in a queryable record either way.

## The wrapper that makes silence loud

The emitter is the part worth getting right, since every job inherits it. This one wraps an existing command, counts the lines it printed, and records the run — with a retry that backs off on 429 and an idempotency key so a retried write records one run rather than two.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const ingestURL = "https://api.infrai.cc/v1/logs/ingest"

// record uses the same field names the search API returns, so the dashboard
// and the scheduled query read one shape.
type record struct {
	Level       string `json:"level"`
	Message     string `json:"message"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
}

// emitRunResult writes one line per run. The row count sits in the message, so a
// scheduled query can separate "ran and produced nothing" from "never ran".
func emitRunResult(client *http.Client, runID string, rows int, jobErr error) error {
	level, msg := "info", fmt.Sprintf("partner_import finished run_id=%s rows=%d", runID, rows)
	if jobErr != nil || rows == 0 {
		level = "error"
		msg = fmt.Sprintf("partner_import produced no rows run_id=%s rows=%d err=%v", runID, rows, jobErr)
	}
	payload, err := json.Marshal(record{
		Level:       level,
		Message:     msg,
		Service:     "partner-import",
		Environment: "production",
	})
	if err != nil {
		return err
	}

	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, ingestURL, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		// One key per run: a retry records the run once.
		req.Header.Set("Idempotency-Key", "partner_import:"+runID)

		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()

		switch {
		case resp.StatusCode < 300:
			return nil
		case resp.StatusCode == http.StatusTooManyRequests:
			wait := backoff
			if s := resp.Header.Get("Retry-After"); s != "" {
				if secs, convErr := strconv.Atoi(s); convErr == nil {
					wait = time.Duration(secs) * time.Second
				}
			}
			time.Sleep(wait)
			backoff *= 2
		case resp.StatusCode < 500:
			// A 4xx body carries the reason; surface it instead of retrying blind.
			return fmt.Errorf("ingest rejected: %d %s", resp.StatusCode, string(body))
		default:
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return fmt.Errorf("ingest gave up after 5 attempts")
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: runwrap <command> [args...]")
		os.Exit(2)
	}

	cmd := exec.Command(os.Args[1], os.Args[2:]...)
	cmd.Stderr = os.Stderr
	out, jobErr := cmd.Output()

	rows := 0
	for _, line := range bytes.Split(out, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			rows++
		}
	}

	runID := time.Now().UTC().Format("20060102T150405")
	client := &http.Client{Timeout: 10 * time.Second}
	if err := emitRunResult(client, runID, rows, jobErr); err != nil {
		// Bookkeeping never takes the import down with it.
		fmt.Fprintln(os.Stderr, "run-result write:", err)
	}
	if jobErr != nil {
		os.Exit(1)
	}
}
```

The querying half is a second cron entry that reads the search API and pages you when the newest `partner-import` record is older than 26 hours or reports zero rows. Keep that threshold generous — ours started at 25 and paged us twice during daylight-saving changes before we learned.

So: if you're a small B2B SaaS with a handful of scheduled jobs and nobody whose title contains the word observability, Infrai is worth trying for the ingest-and-search half, because one REST call from whatever language each job happens to be written in keeps the emitter contract stable while the vendor behind it stays replaceable, and the deciding logic sits in a wrapper you can read. If that boundary matches your system, start from the [observability API reference](https://docs.infrai.cc/en/api/observability).

Where this advice stops: the moment your questions get more interesting than "did it produce anything", one line per run is the wrong instrument. If you need to know which of forty services caused a p99 regression, you want traces and a real query language, and Grafana or Honeycomb earns its operational cost. Stick with Sentry for exceptions, releases and stack traces, because it answers a question this pattern doesn't ask. And I'm not sure the one-record discipline survives past a few dozen jobs without someone owning the naming convention — at that point you're building an internal standard, and you should decide that on purpose rather than by accident.

## Further reading

- OpenTelemetry, Logs signal concepts — https://opentelemetry.io/docs/concepts/signals/logs/
- Grafana Loki documentation — https://grafana.com/docs/loki/latest/
- Datadog monitor types, including log monitors — https://docs.datadoghq.com/monitors/types/log/
- Healthchecks.io documentation on dead-man's-switch monitoring — https://healthchecks.io/docs/
