# Transactional Welcome Email API: Reusable Templates for Property Inquiry Batch Routing

TL;DR: Treat the template as a versioned routing policy owned by the platform team, while treating the delivery API as a replaceable transport. For a property-management contact form, accept one inquiry at a time, persist the routing decision and template revision, enqueue an immutable message command, and let a rate-limited worker deliver it. Use batch submission only for an explicitly requested onboarding series, never to compensate for a missing queue. This keeps welcome-email wording reusable without giving a provider dashboard control over support-queue selection, compliance text, or rollback.

The API decision follows from that boundary. A suitable service must accept an idempotency key or tolerate application-side deduplication, expose delivery events, preserve a stable recipient identifier, and let the team promote a tested template revision without editing production content in place. Throughput claims matter only after measuring arrival bursts, worker concurrency, and the downstream quota. The harder question is who can change meaning at 02:00 and how quickly that change can be reversed.

## How should a transactional welcome email API handle reusable templates?

A property contact form usually carries more operational meaning than its small payload suggests. A prospective tenant asking about `leasing` belongs with a leasing queue; an existing resident reporting `maintenance` belongs with operations; an owner asking about `statements` belongs with accounting. The acknowledgement email may be delivered perfectly while the internal handoff is wrong because routing and rendering used different interpretations of the same free-form fields. Delivery success is then a misleading signal.

Make the classification result explicit. Store a normalized property identifier, inquiry category, destination queue, policy version, locale, template revision, and a generated message ID beside the original submission. Do not make a worker classify the inquiry again from prose. The worker should render a known revision from recorded variables and send the resulting command; retries must repeat that command, not re-run business policy that may have changed in the meantime.

Short paths fail too.

Sending directly from the form handler couples user latency to a remote dependency and creates an ambiguous retry boundary: if the client disconnects after acceptance, neither side knows whether another submission is safe. Hiding that ambiguity behind a provider's bulk endpoint does not remove it. A durable queue, a unique application message ID, and a delivery ledger turn the ambiguity into states the on-call engineer can inspect.

Capacity planning starts at the form, not at an advertised messages-per-second figure. Let peak accepted inquiries be `A` per second, let each inquiry create `M` messages, and reserve a retry multiplier `R` from observed failure data. Required steady worker throughput is `A * M * R`. Queue-age SLOs then constrain concurrency. Pick the actual values from traffic and delivery telemetry; inventing a universal retry multiplier would hide the risk rather than quantify it.

Consider a planning worksheet, not a benchmark: if an observed peak is 12 accepted forms per second, each form produces one resident acknowledgement and one internal notification, and the team's measured retry allowance is 1.25, the worker target is `12 * 2 * 1.25 = 30` message attempts per second. That arithmetic does not prove a transport can sustain 30 attempts per second, nor does it prescribe those inputs for another portfolio. It gives the load test a falsifiable target. The test still has to include the chosen payload sizes, both template revisions, event callbacks, per-domain throttling behavior, and a burst long enough to reveal queue growth. If the oldest-command SLO is 60 seconds and intake briefly exceeds service capacity, the team can calculate how much backlog is tolerable before admission control or batch suspension must engage. Keep onboarding work in a lower-priority queue so an optional sequence cannot consume the capacity reserved for a tenant who has just submitted a maintenance request. This is a deliberate trade-off: slower onboarding is acceptable; hiding a time-sensitive support inquiry behind it is not.

## Put template ownership on the architecture diagram

“Templates supported” is not a decision criterion. Ownership is. There are three plausible control planes, and each shifts a different page to the on-call rotation.

| Template model | Change authority | Rollback unit | Operational trade-off |
|---|---|---|---|
| Repository-owned rendering | Code owners and deployment policy | Application artifact or template revision | Strong review and reproducibility; the team owns rendering, localization, and preview tooling |
| Delivery-service rendering | Service roles and dashboard/API policy | Remote template version | Faster content-only changes; correctness now depends on remote version semantics, access control, and audit export |
| Split ownership | Application owns routing and variables; delivery service owns presentation | Coordinated policy and template revisions | Useful for specialist content teams, but requires a compatibility contract and two-part rollback |

For support-queue routing, repository ownership is the conservative default because destination selection is business policy rather than presentation. A split model can still work if the remote template receives a closed, validated variable set and cannot choose the queue, sender identity, or message class. The transport should not infer those from copy.

I would reject any evaluation that ends with a feature checkbox. Ask each candidate transport to demonstrate how immutable versions are addressed, how event authenticity is verified, how long event data remains available, what happens when a template revision is missing, and whether account-level throttling can isolate transactional acknowledgements from an onboarding batch. Those answers determine the runbook. A polished editor does not.

The buy-versus-build line should be equally explicit.

| Capability | Usually buy | Usually retain in-house | Reason |
|---|---:|---:|---|
| Mail transfer and recipient-domain handling | Yes | No | It is transport work with broad external dependencies |
| Inquiry classification and queue mapping | No | Yes | It encodes property operations and changes with the organization |
| Template rendering | Depends | Depends | The answer follows authorship, review, localization, and rollback needs |
| Durable command queue and message ledger | No | Yes | They define application idempotency and incident evidence |
| Delivery event ingestion | Endpoint plumbing | State interpretation | A service can emit events; the application decides what they mean for its SLO |

This is also the lock-in test. Replacing transport should require a new adapter and event mapper, not a rewrite of inquiry classification, template variables, or audit history.

## Implement an immutable send command

The following Go example shows the narrow boundary. It deliberately omits a specific provider SDK. `Mailer` can be backed by any transport that can accept a fully rendered message, while the command records the policy and template versions that produced it. The message ID is generated before enqueueing and reused on every attempt.

```go
package messaging

import (
    "context"
    "errors"
    "fmt"
    "net/mail"
)

type WelcomeCommand struct {
    MessageID       string
    InquiryID       string
    Recipient       string
    DestinationQueue string
    PolicyVersion   string
    TemplateVersion string
    Subject         string
    TextBody        string
    HTMLBody        string
}

type Mailer interface {
    Send(ctx context.Context, command WelcomeCommand) (transportID string, err error)
}

type Ledger interface {
    Delivered(ctx context.Context, messageID string) (bool, error)
    RecordAccepted(ctx context.Context, messageID, transportID string) error
}

func Deliver(ctx context.Context, m Mailer, ledger Ledger, cmd WelcomeCommand) error {
    if cmd.MessageID == "" || cmd.InquiryID == "" || cmd.PolicyVersion == "" || cmd.TemplateVersion == "" {
        return errors.New("message command is missing immutable identity")
    }
    if _, err := mail.ParseAddress(cmd.Recipient); err != nil {
        return fmt.Errorf("invalid recipient: %w", err)
    }

    sent, err := ledger.Delivered(ctx, cmd.MessageID)
    if err != nil {
        return fmt.Errorf("read delivery ledger: %w", err)
    }
    if sent {
        return nil
    }

    transportID, err := m.Send(ctx, cmd)
    if err != nil {
        return fmt.Errorf("send message %s: %w", cmd.MessageID, err)
    }
    if err := ledger.RecordAccepted(ctx, cmd.MessageID, transportID); err != nil {
        return fmt.Errorf("record accepted message: %w", err)
    }
    return nil
}
```

This function does not claim exactly-once delivery. There is still a gap between remote acceptance and the local ledger write. Close it with a transport-supported idempotency key when available; otherwise reconcile by message ID and provider event before retrying ambiguous attempts. The application ledger must distinguish `queued`, `attempting`, `accepted`, `delivered`, `deferred`, `bounced`, and `suppressed` rather than flattening every non-error response into success. Exact labels may differ at the adapter, but the internal state machine should not.

Batch onboarding needs a separate scheduler that emits one command per recipient and per step. Keep campaign membership, consent or other applicable sending basis, schedule, and cancellation state outside the template. A batch request is an optimization at the adapter boundary; it must not erase per-recipient identity or prevent one address from being suppressed.

Welcome traffic and promotional traffic also need an explicit classification review. The FTC explains that CAN-SPAM applies to commercial email, gives recipients the right to stop such mail, and sets requirements including accurate headers, non-deceptive subjects, a valid physical postal address, and honoring opt-out requests. It also says the primary purpose of a message determines whether it is commercial or transactional/relationship content. Calling a campaign “welcome” does not settle that question. Keep the acknowledgement focused on the requested property inquiry, and have counsel or the responsible compliance owner review any promotional onboarding series.

Security messages deserve their own lane. OWASP recommends consistent responses and timing for account-recovery requests, side-channel delivery, rate limiting, and single-use expiring tokens. A property inquiry acknowledgement should never share a template or batch with a password-reset message merely because both are “transactional.” Their abuse controls and failure consequences differ.

## Verify the route, not merely the request

The release gate should exercise the full decision chain against a non-production sink: form fixture, normalized property, category, destination queue, rendered MIME content, adapter request, signed event ingestion, and ledger transition. Golden-file tests are useful for stable text and HTML output, while contract tests should fail when a required variable disappears or an unknown variable arrives. Parse addresses and MIME output instead of checking them with string fragments.

Use a small matrix with disproportionate failure value: one leasing inquiry, one maintenance inquiry, one accounting inquiry, an unknown property, an unsupported locale, a duplicate submission, and a recipient already marked as suppressed. Verify that the unknown cases stop before delivery and enter a visible review queue. Silent fallback to a default property is operationally convenient and semantically dangerous.

The primary service-level indicator is the proportion of accepted inquiries that reach the correct internal queue and receive an acknowledgement within the defined window. Delivery-event latency, bounce rate, suppression count, retry count, oldest queue age, and dead-letter volume are supporting signals. Measure by message class and property portfolio; a healthy aggregate can conceal a broken routing rule for one building. Alert on sustained SLO burn and queue age, not each transient transport error.

Correlation must survive every hop. Log the inquiry ID, application message ID, policy version, template version, attempt number, and transport ID as structured fields, while keeping addresses and message bodies out of routine logs. RFC 5322 defines the Internet Message Format and the `Message-ID` field; keep a separate application ID as the durable join key because adapters and downstream systems may expose their own identifiers. Delivery Status Notifications are specified by RFC 3464, but event normalization still belongs in the adapter.

Before release, verify both plain-text and HTML bodies, header construction, reply handling, localization fallback, event signature rejection, duplicate event handling, timeouts, and cancellation. Then run a limited cohort whose size is chosen from the team's error budget and rollback detection time, not from a generic percentage.

## Roll back meaning before changing transport

Template and policy revisions should be independently selectable but deployed as a tested pair. When a new version misroutes inquiries or renders invalid content, stop new batch expansion, pin the scheduler to the last known-good pair, and leave already accepted commands immutable. Rewriting queued commands destroys the evidence needed to determine who received what; cancel and regenerate them under a new message ID only when policy requires correction.

Transport failover is a later move because it changes authentication, throttling, event semantics, bounce classification, and possibly sender reputation behavior at once. Use it for a demonstrated transport impairment after the adapter has passed its failover contract tests. A copy regression calls for a template rollback. A queue-map regression calls for a policy rollback. Neither justifies changing the mail path during the incident.

Recovery ends with reconciliation. Compare accepted inquiry IDs with message-ledger records, account for every ambiguous attempt, replay only commands whose state permits it, and confirm that delayed events cannot regress a terminal state. Record the affected policy and template revisions in the incident timeline. Then adjust tests or ownership controls where the bad change entered; raising a retry limit will not repair a meaning error.

The durable choice is therefore a transport that fits this control model, rather than the transport with the longest feature list. Keep routing policy and evidence where the platform team can review, test, observe, and roll them back; let a delivery service carry messages across the mail boundary; and require any campaign-like onboarding flow to preserve the same per-recipient controls.

## References

- RFC 5322, Internet Message Format: https://www.rfc-editor.org/rfc/rfc5322
- RFC 3464, An Extensible Message Format for Delivery Status Notifications: https://www.rfc-editor.org/rfc/rfc3464
- OWASP Forgot Password Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- FTC CAN-SPAM Act compliance guide for business: https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
- Go `net/mail` package documentation: https://pkg.go.dev/net/mail
