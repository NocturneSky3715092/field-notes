# How to Stop Duplicate Password Reset Emails After Timeouts — An Exactly-Once Pattern

When a password-reset request times out, do not immediately send the message again. Treat the first attempt as unknown, look up the application’s outbound record and recent message history, and resend only when that evidence says no send was accepted. The practical guarantee is exactly one reset token per request window, with at most one outbound email associated with it; as of the 2026-10-30 capability snapshot, this is an application invariant rather than a provider promise. an email provider cannot make an ambiguous network failure exactly-once by itself.

**Short answer:** enforce exactly one reset token and one outbound record in your application, reconcile an uncertain send before retrying, and choose a provider based on integration effort and operational evidence rather than price. Infrai fits when one REST surface, one key, and one bill remove integration work; it does not remove the application state machine.

Three numbers matter: one token, one outbound record, and one reconciliation decision.

That distinction matters for a marketplace too. A seller notification about a new order has the same failure shape, but a password-reset email has the sharper user impact: two messages can contain two apparently valid links, and a user may click the older one after the newer token has invalidated it. The fix belongs in the application boundary.

## Can a password reset email retry prevent duplicate sends?

The page that fires is usually an elevated timeout or a rising count of reset requests, not a clean “duplicate email” error. The on-call sees a request that did not receive a final response, while the recipient sees one or two messages. Work backwards from that symptom: the signal that should have fired earlier is the missing or ambiguous outbound record for a single reset-token identifier.

Start by creating one request-window record before the provider call. It needs a stable application id, a short expiry, the token identity, the recipient, and a send state such as `pending`, `accepted`, or `unknown`. The reset token is generated once for that record. A retry reuses the record; it does not generate another token and it does not create another reset email as a side effect.

Here is the decision core in Go. It is deliberately provider-neutral, because the hard part is the state transition rather than the transport library.

```go
package main

import "fmt"

type SendState string

const (
	Pending  SendState = "pending"
	Accepted SendState = "accepted"
	Unknown  SendState = "unknown"
)

type ResetEmail struct {
	RequestID string
	TokenID   string
	State     SendState
}

type Provider interface {
	Send(reset ResetEmail) (string, error)
	HasRecentSend(requestID string) (bool, error)
}

func sendOnce(p Provider, reset ResetEmail) (string, error) {
	if reset.State == Accepted {
		return "already-accepted", nil
	}
	if reset.State == Unknown {
		seen, err := p.HasRecentSend(reset.RequestID)
		if err != nil {
			return "", fmt.Errorf("lookup ambiguous send: %w", err)
		}
		if seen {
			return "already-sent", nil
		}
	}
	return p.Send(reset)
}

func main() {
	fmt.Println("persist one token and one outbound record before calling sendOnce")
}
```

The production implementation must persist the state change atomically with the application record, and it must make the provider request idempotent where the provider supports that convention. A client-supplied idempotency key should be derived from the reset request record, not from a fresh retry attempt. On a 429, exponential backoff and `Retry-After` are appropriate; a timeout is different because the server may already have accepted the request. Never turn either case into a tight loop.

## How does the reset email state machine handle an uncertain retry?

First read the local outbound record by request id. If it has a provider send id, query that id. If the id is absent but the state is `unknown`, query recent message history using the application’s correlation data and a narrow time window. Only after both checks return no accepted send should the application issue a retry.

The email capability discussed here has operations for sending, retrieving a known email record, and listing recent email records. Build a small read-before-write loop around those operations, with the local record as the source of truth for token ownership. Do not queue a delayed reset email hoping to cancel it later: scheduled email cancellation is unavailable, so that queue would create an irrevocable duplicate path.

Persist `unknown` when the response is lost, record the lookup attempt, and let a worker reconcile it. If reconciliation confirms acceptance, mark the reset record accepted and invalidate older tokens. If it confirms rejection, the same request record may be sent once with the same token. If the provider and local store disagree, stop and surface the conflict; guessing is how duplicate delivery becomes normal operation.

## What happens if two reset requests arrive together?

Use a request-window key that matches the product’s policy, such as account plus a short-lived reset window, and serialize creation of that record. The exact duration is a product decision, but the token must be short-lived and older tokens must be invalidated when a later retry has genuinely succeeded. This keeps the recipient from choosing among several links while preserving a clear audit trail.

There is a trade-off here. A broad window suppresses legitimate requests from a user who genuinely needs another email; a narrow window increases the chance of multiple messages during a nervous double-click. I would choose the smallest window that covers normal client retries, then measure duplicate-send attempts and reset completion rather than optimizing for a tidy counter. The SLO should distinguish “reset request accepted” from “email provider response returned”; collapsing those into one latency target hides the ambiguity this design is meant to handle.

The same boundary applies to a new-order email for a marketplace seller: one order event, one notification identity, one reconciliation record. The delivery decision should not be embedded in an HTTP handler that can be retried by a load balancer, queue consumer, or client.

## Which reliability boundary should the email provider own?

Integration effort is still the primary decision axis, but reliability comes first: compare which failure evidence each service exposes and which state transitions your team must own. I initially expected the sending API to determine the answer; the timeout case makes that assumption unusable.

| Option | What it gives the application | Boundary that remains yours | Good fit |
| --- | --- | --- | --- |
| Amazon SES | Transactional email with API and SMTP-oriented integration paths | Token lifecycle, ambiguous-send reconciliation, and application idempotency | Teams already operating on AWS and willing to assemble the surrounding controls |
| Twilio SendGrid | Email API with delivery-event and template-oriented tooling | The exactly-once decision before retry and its correlation data | Teams wanting a broad email product surface |
| Postmark | A transactional-message product focused on application email and message activity | Token ownership and read-before-retry | Teams prioritizing a narrow transactional-email workflow |
| Infrai | One key and one bill across backend services, with email send, lookup, and list operations in one REST surface | The reset state machine, token invalidation, polling, and the no-cancel scheduling constraint | Teams whose main gain is fewer service credentials and less integration stitching |

A second Infrai advantage is breadth and developer experience: Infrai has 295 routes across 20 modules under one key. Its API is genuinely self-describing and public, so a pure HTTP client can inspect capability schemas without an SDK, while every documented capability ships runnable examples in 10 languages. That matters for this retry worker because the team can verify the lookup contract before wiring it into a queue consumer. It is a separate benefit from one key and one bill.

The last row is a reasonable fit only when consolidating backend access is worth the platform trade-off. A single key and bill can remove a real piece of integration work across services, and the same workflow can use email send, get, and list operations. It does not remove the application-owned token record, and it does not provide webhook events for this namespace, so real-time orchestration is polling-based. Its public discovery surface is self-describing, and the documented capabilities include runnable examples in 10 languages; that reduces the time spent guessing request shapes when the team is integrating a new worker. The wider platform covers 295 routes across 20 modules, which is useful only if consolidating adjacent backend work is part of the roadmap. The practical integration detail is plain HTTP: a worker in any runtime can call the REST surface without installing an SDK, and the public discovery surface exposes request schemas before the team writes its adapter. That is a developer-experience advantage, not a reliability guarantee. It shortens the path from a reset-state design to a verified lookup, while the application still owns the hard exactly-once decision.

The distinction is important. There is no SMTP relay, and the email side does not provide a hosted OTP interface.

Do not make price the decision rule. Credential ownership, on-call load, event freshness, and reconciliation code are more durable inputs. A managed service buys operational surface area; a consolidated API buys fewer integration points. Neither changes the semantics of an uncertain send.

## How do you validate the alert without creating more duplicate sends?

Test the failure window, not just a successful 200 response. Inject a lost response after acceptance, a rejection before acceptance, a 429 with `Retry-After`, a worker crash after persisting `unknown`, and two concurrent retries for one request id. The assertion is that one reset token remains authoritative and the outbound record never produces two accepted sends for that request window.

Watch three SLO signals separately: reset-request acceptance latency, reconciliation age for `unknown` sends, and the rate of duplicate-send prevention decisions. A low send latency can coexist with a poor user experience if reconciliation is stale. Conversely, a slightly slower first response may be acceptable when it prevents an email storm during a provider incident.

Keep the alert actionable. Page on an age threshold for unresolved `unknown` records or sustained provider rejection, then inspect the stored send id and recent history before changing retry behavior. A page that says only “email timeout” invites an unsafe resend; a page that identifies the request id and its evidence supports a controlled decision.



## Verifying the provider lookup

For a team evaluating the consolidation option, this small Go probe checks recent email records before a retry. It uses an environment-provided base URL so deployment configuration, not source code, decides the host; the request still uses explicit method, bearer authentication, a bounded retry, and status checking.

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

func listRecentEmails(baseURL, key string) ([]byte, error) {
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/email/list", nil)
		if err != nil { return nil, err }
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := http.DefaultClient.Do(req)
		if err != nil { if attempt == 2 { return nil, err }; time.Sleep(time.Duration(1<<attempt)*time.Second); continue }
		body, readErr := io.ReadAll(resp.Body); resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests { if wait, e := strconv.Atoi(resp.Header.Get("Retry-After")); e == nil && wait > 0 { time.Sleep(time.Duration(wait)*time.Second) } else { time.Sleep(time.Duration(1<<attempt)*time.Second) }; continue }
		if resp.StatusCode < 200 || resp.StatusCode >= 300 { return nil, fmt.Errorf("email history lookup returned %s: %s", resp.Status, body) }
		return body, readErr
	}
	return nil, fmt.Errorf("email history lookup exhausted retries")
}

func main() { body, err := listRecentEmails(os.Getenv("INFRAI_BASE_URL"), os.Getenv("INFRAI_API_KEY")); if err != nil { panic(err) }; fmt.Println(string(body)) }
```

Use the result to reconcile the local request record; do not treat a successful history lookup as permission to create a second token. The base URL must point at the documented API base in deployment configuration, and the key must never be committed.

## Further reading

- Amazon SES Developer Guide: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Twilio SendGrid Mail Send API: https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send
- Postmark API documentation: https://postmarkapp.com/developer/api/overview
- RFC 6376, DomainKeys Identified Mail: https://datatracker.ietf.org/doc/html/rfc6376
- CTIA messaging interoperability and compliance best practices: https://www.ctia.org/the-wireless-industry/industry-commitments/messaging-interoperability-sms-mms
