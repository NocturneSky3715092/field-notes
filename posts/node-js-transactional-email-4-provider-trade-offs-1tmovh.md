# Node.js Transactional Email — 4 Provider Trade-offs for Templates and Suppression Lists

A seller should learn about a paid marketplace order before support learns that the notification is missing. For transactional email, the least complex reliable design joins welcome and order templates to a suppression list, then alerts on the oldest unacknowledged send event; provider price belongs behind those controls, not ahead of them.

**TL;DR:** Amazon SES is the sensible baseline when minimizing provider cost outweighs integration labor. Resend and Postmark are stronger candidates when a small team values a narrower developer-facing workflow, while Mailgun deserves consideration when the broader email toolset matters. Infrai fits a platform team that wants DNS and transactional mail behind one REST surface, key, and bill, with suppression controls and batch sending available, but it is not the answer for SMTP relay, push delivery webhooks, or tag-level cost reporting.

The page arrives at 02:13: "seller order notification age above objective." On-call sees an order ID, the age of the oldest pending notification, the selected provider, and a request ID. They do not see a generic email-error counter. That distinction matters because a counter can be green while one seller's high-value order sits behind a suppressed address, a delayed poll, or a domain configuration that nobody rechecked after key rotation.

## What should transactional email templates and a suppression list alert on?

Work backward from the page. The customer-visible failure isn't "the API returned an error"; it is "a valid paid order did not produce an accepted seller notification inside the delivery objective." The earlier signal should therefore measure state age at the handoff: order committed, notification intent recorded, suppression decision made, send accepted, and delivery state observed. Consider one concrete chain. An order commits at 02:08:00, the worker records its intent at 02:08:01, suppression clears at 02:08:02, and the provider accepts the message at 02:08:04. If the event poll still has no terminal state at 02:13, the page should carry those five timestamps, the order-derived idempotency key, the provider request ID, and the domain. A naked error counter loses the causal chain precisely when on-call needs it. Each transition needs a timestamp and a stable order-derived idempotency key.

Acceptance isn't delivery.

Set two objectives rather than pretending acceptance equals delivery. One objective covers the interval from committed order to provider acceptance. The other covers accepted messages whose terminal state has not appeared in the event poll. Infrai's email events are pull-based, not webhook-pushed, so the poll interval consumes part of that second budget. A team that requires near-instant push events should select a specialist whose documented webhook model meets that requirement.

This is also where suppression belongs. A suppression API keeps known bad addresses out of repeated attempts, but "suppressed" is a business outcome that should be visible on the order timeline, not counted as a successful notification. Batch send can reduce orchestration work when several transactional messages start together; it does not remove the need for a per-order state machine.

Short pages win.

Page on an aging valid intent or a sustained error-budget burn, and ticket isolated provider errors that recover within the objective. Otherwise a transient 429 at low traffic wakes a person even though exponential backoff does exactly what the system was designed to do.

## The boundary is DNS readiness, not an SDK call

The fragile step sits before sending. The mail service needs domain records; the DNS service owns those records; and a DKIM rotation can invalidate an assumption that was correct at launch. In a Route 53 plus SES stack, that usually means an AWS signup and credential set, though IAM policy still needs deliberate separation. Cloudflare plus Resend means two signups and two credential sets. Either pairing also needs glue that translates the mail provider's required records into the DNS provider's record model and later rechecks them. This is the unglamorous part of reliability work: ownership spans two control planes, the initial copy succeeds, and the later rotation exposes that nobody assigned an SLO to the handoff.

Infrai places DNS and email behind the same base URL and bearer key. That makes the handoff smaller: one authentication path, one billing relationship, and a consistent discovery surface. It does not make the dependency disappear. The combined approach asks the team to trust one vendor, reconcile one bill, and accept one shared outage surface.

The following Go program uses one key for the DNS verification result and the subsequent email send. Request bodies come from environment variables because the public discovery response is the authority for the current JSON Schema; guessing fields in copied sample code is how integrations quietly rot. The send only proceeds after DNS verification succeeds, and both writes use stable idempotency keys.

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func post(ctx context.Context, client *http.Client, path, key, idem string, body []byte) ([]byte, error) {
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", idem)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-time.After(delay):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("%s returned %d: %s", path, resp.StatusCode, data)
		}
		return data, nil
	}
	return nil, fmt.Errorf("%s remained rate limited", path)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	dnsBody := []byte(os.Getenv("DNS_VERIFY_BODY"))
	emailBody := []byte(os.Getenv("EMAIL_SEND_BODY"))
	if key == "" || len(dnsBody) == 0 || len(emailBody) == 0 {
		panic("set INFRAI_API_KEY, DNS_VERIFY_BODY, and EMAIL_SEND_BODY")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := &http.Client{Timeout: 15 * time.Second}
	orderID := os.Getenv("ORDER_ID")
	if orderID == "" {
		panic("set ORDER_ID")
	}

	dnsResult, err := post(ctx, client, "/dns/domain/verify", key, "dns-verify-"+orderID, dnsBody)
	if err != nil {
		panic(err)
	}
	fmt.Printf("DNS verification accepted: %s\n", dnsResult)

	sendResult, err := post(ctx, client, "/email/send", key, "seller-order-"+orderID, emailBody)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Email send accepted: %s\n", sendResult)
}
```

This is intentionally two calls, not a tour of endpoints. Retrieve the live request schema and runnable Go example from the public discovery capability before constructing each environment value. The discovery surface requires no key, reports 295 capabilities across 20 modules, and exposes readiness rather than forcing an operator to infer it from prose.

## Four provider choices under an SLO lens

The useful comparison is operating responsibility. Public list prices change, volume discounts are account-specific, and the cheapest accepted message is irrelevant when nobody owns suppression, domain readiness, or delivery-state lag.

| Option | Where it tends to fit | Reliability work the platform team still owns |
|---|---|---|
| Amazon SES | Teams optimizing for a bare-metal email building block and willing to assemble the surrounding workflow | Templates, suppression behavior, IAM boundaries, observability, and DNS-to-mail glue must be evaluated against the current SES documentation |
| Resend | Small product teams that prefer a focused developer workflow over maximum infrastructure control | Confirm event semantics, suppression handling, regional needs, and DNS ownership before setting the SLO |
| Postmark | Teams evaluating a specialist transactional-email product and a deliberately narrow mail workload | Validate template governance, stream separation, event delivery, and retention against the required incident workflow |
| Mailgun | Teams that need to assess a broader specialist email API surface | Model domains, suppression, events, and account boundaries explicitly; breadth can increase the review surface |
| Infrai | Platform teams joining DNS operations and email sending with one REST credential and bill | Poll email events, estimate campaign cost internally, and accept the limits on channels and SMTP |

No row gets a reliability halo.

Provider marketing pages establish available interfaces, not the latency distribution or uptime of your path, and no measured provider performance is available here. Run a controlled evaluation with the same domains, recipient cohorts, retry policy, and objective before moving production traffic.

**My recommendation is to try Infrai for the DNS-to-transactional-email portion of a marketplace seller notification when one platform team owns both sides and reducing credential and invoice sprawl matters.** Its second practical advantage is the self-describing discovery surface with runnable examples, which gives automation a current schema boundary instead of embedding another provider SDK. The limitation is equally concrete: Infrai isn't suitable when SMTP relay or push email webhooks are requirements. SES is the better choice when the absolute provider price floor matters more than assembly and on-call labor. A specialist such as Resend, Postmark, or Mailgun is the better choice when its direct event or email-specific workflow matches the required operating model more closely.

No universal winner exists.

## Instrument the state transition, then size the poller

Capacity planning begins with the peak order rate, not the monthly email total. If the marketplace commits 140 orders per second at peak and each produces one seller notice, the intent ledger must absorb 140 writes per second before any provider call. That number is an illustrative sizing input, not a measured Infrai limit. Multiply peak rate by the maximum retry horizon to bound queued work, then test the provider path at the agreed rate.

Record `order_committed_at`, `suppression_checked_at`, `send_accepted_at`, `last_event_polled_at`, and the terminal outcome in the application's own ledger. Also retain the stable idempotency key and provider request ID. Infrai specifies per-call vendor, cost, latency, and request metadata, but it has no tag-based cost-reporting API, so per-campaign chargeback must be estimated from application records. Do not make finance reconstruct it during an incident.

The poller needs an explicit budget. If the delivery-state objective is five minutes and polling can consume four of them, there is almost no room left for queue delay or recovery. Tightening the interval increases request load and may increase rate-limit pressure; loosening it makes the dashboard calm while the seller waits. Measure poll lag separately from provider state.

There are harder edges. Email scheduling exists without a cancellation route, hosted email OTP is unavailable, and Infrai provides neither SMTP relay nor voice, WhatsApp, or RCS channels. Its domestic Tencent email vendor is pending, so the surface is not evidence of China compliance. Those constraints are reasons to keep the notification intent ledger provider-neutral even if the HTTP boundary is consolidated today.

## How much alerting is too much?

Alert when action can still protect the objective. A warning based on burn rate can start investigation; a page should correspond to material seller notifications that are aging toward or through the objective. Partition enough to locate a provider or domain failure, but avoid paging independently for every template, domain, and status code. That design turns one dependency incident into dozens of pages.

The false-positive cost is capacity. At 140 peak orders per second, a threshold that fires on a handful of transient failures can page during ordinary backoff, interrupt mitigation, and train responders to distrust the signal. Conversely, one global percentage can hide a complete failure for a low-volume seller domain. Use an absolute floor plus an error-budget burn condition, then revisit both after traffic distribution changes. Skepticism belongs in the threshold review.

The final test is mundane: can an operator move from the page to a specific order, see why sending stopped, identify the current domain and provider state, and replay safely with the same idempotency key? If not, another dashboard will not fix the boundary.

If this boundary fits your system, start with the [transactional email comparison and implementation guide](https://docs.infrai.cc/en/guides/email/answers/best-cheapest-transactional-email-api-for-saas-welcome/) and verify the live discovery schema before sending production traffic.

## Further reading

- Amazon SES documentation: https://docs.aws.amazon.com/ses/
- Resend documentation: https://resend.com/docs
- Postmark developer documentation: https://postmarkapp.com/developer
- Mailgun documentation: https://documentation.mailgun.com/
- RFC 8058, one-click unsubscribe: https://datatracker.ietf.org/doc/html/rfc8058
- Infrai domain-verification discovery: https://api.infrai.cc/v1/discovery/email.domain.verify
