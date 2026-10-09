# Game Marketplace Password Reset Email Provider Contracts for Domain Bounce Handling

Make the provider earn its place by passing an integration contract, not by winning a feature checklist. For a game-item marketplace, that contract must prove that password-reset mail keeps its authenticated custom-domain identity, that bounces and complaints become durable suppression decisions, and that a surge of seller order notices cannot starve account recovery. **TL;DR:** own the queue, message correlation, and suppression state; put a narrow adapter around the sender; then accept a managed or self-hosted transport only after the same tests pass against it.

Integration effort is the deciding constraint. Sending one message is trivial compared with reconciling delayed events, rotating DNS-backed identity, replaying callbacks, and replacing a transport while sellers are trying to regain access to fulfill new orders. The useful comparison is therefore the number of application contracts a choice forces the team to change, plus the operational load those contracts add.

## How should a provider handle password reset email deliverability?

Start at the recovery outcome. OWASP recommends consistent responses for existing and nonexistent accounts, uniform response timing, side-channel delivery of reset instructions, random single-use tokens that expire, and rate limiting against excessive requests. Those controls belong to the identity service. The mail layer should never decide whether an account exists, extend token life, or log the reset URL.

The transport contract is smaller than most provider SDKs. It accepts an opaque attempt identifier, an idempotency key, a stream class, an expiry time, and rendered content; it returns evidence of acceptance, not a claim of inbox delivery. A separate event contract records later outcomes. This distinction matters because SMTP acceptance, final delivery, inbox placement, and successful password reset are different signals.

Name the clocks separately: request time, queue time, transport acceptance time, disposition time, and reset completion time. Then define an SLO on the user outcome and use the intermediate clocks for diagnosis. No universal percentage belongs here. A platform team should baseline its own recipient mix and volume, choose a completion window inside the token lifetime, and set an error budget that does not page on a handful of abandoned attempts.

Acceptance proves little.

## Encode the boundary before evaluating a sender

The adapter below keeps security policy out of transport-specific code and makes one capacity rule explicit: an expired reset must not leave the queue. It also assigns password resets a distinct stream from seller order notifications, which allows independent queue reservations and burn-rate alerts when a marketplace event creates an order burst.

```go
package recoverymail

import (
	"context"
	"errors"
	"time"
)

type Stream string

const (
	Recovery    Stream = "account_recovery"
	SellerOrder Stream = "seller_order"
)

type Message struct {
	AttemptID     string
	IdempotencyID string
	Stream        Stream
	Recipient     string
	Subject       string
	HTML          []byte
	ExpiresAt     time.Time
}

type Acceptance struct {
	TransportID string
	AcceptedAt  time.Time
}

type Transport interface {
	Send(context.Context, Message) (Acceptance, error)
}

func SendRecovery(ctx context.Context, transport Transport, message Message, now time.Time) (Acceptance, error) {
	if message.Stream != Recovery {
		return Acceptance{}, errors.New("message is not account recovery")
	}
	if message.AttemptID == "" || message.IdempotencyID == "" {
		return Acceptance{}, errors.New("missing correlation identifiers")
	}
	if !message.ExpiresAt.After(now) {
		return Acceptance{}, errors.New("recovery token has expired")
	}
	return transport.Send(ctx, message)
}
```

Retries reuse the idempotency key and stop at expiry. Correlation metadata must exclude the token and reset URL. The event consumer then maps transport-specific payloads into a compact internal vocabulary such as temporary failure, permanent failure, complaint, and delivery, while retaining the raw signed event for audit and replay.

This creates deliberate work up front. It is still usually less work than allowing SDK types, callback schemas, and suppression semantics to spread through the identity service, queue workers, dashboards, and support tools. The exception is a small system with no realistic replacement requirement and no team to operate reconciliation; there, one managed integration plus documented exports can be the more honest commitment.

## Authenticate the domain and preserve suppression state

SPF, DKIM, and DMARC answer related but different questions. RFC 7208 defines SPF authorization for sending hosts, RFC 6376 defines DKIM's domain-associated signatures, and RFC 7489 defines DMARC alignment between the visible From domain and authenticated identifiers plus a requested handling policy. None guarantees inbox placement.

Test the exact production path by inspecting received headers from controlled mailboxes. Verify the visible From domain, return path, DKIM selector and signature result, SPF result, DMARC alignment, and reset-link host. A DNS record existing somewhere is weak evidence; the deployed message has to use the intended identity. Rotation deserves its own drill because old and new DKIM selectors may need to coexist while cached DNS data ages out.

Suppression is an application state machine, not a provider checkbox. Consider a reset accepted at 10:00, followed by a temporary-failure event at 10:01, a permanent-failure event at 10:03, and then a delayed duplicate of the temporary event. Processing in arrival order without transition rules would reopen a destination already known to be bad. Instead, key events by their source identifier, retain event time separately from ingestion time, make the permanent disposition terminal unless an authorized process clears it, and record that override. A complaint takes a different terminal path and must never enter ordinary retry logic. This concrete four-event sequence belongs in every adapter test because a happy-path send cannot expose the error.

State has memory.

Do not merge seller-order and recovery policy merely because both use email. An order notice can be retried after a longer delay. A reset link has an expiration boundary, and delivering it after that boundary creates a dead end for the user.

## Choose the operational burden explicitly

The buy-versus-build decision is mostly a decision about who carries queue operations, key custody, sender reputation, abuse response, feedback ingestion, and data retention. A short send method can hide a long pager rotation.

| Decision surface | Managed transport | Self-operated transport | Contract evidence |
|---|---|---|---|
| Initial integration | Provider schema and hosted sending reduce the first implementation | Team builds and operates the delivery path | Adapter test passes without application changes |
| Domain identity | Guided setup may reduce DNS and signing work | Team owns selectors, keys, rotation, and outbound configuration | Received headers prove authentication and alignment |
| Event handling | Callback format, ordering, and retention are external boundaries | Team owns collection, storage, and classification | Signed fixtures can be replayed idempotently |
| Suppression | Lower setup effort, with portability dependent on export detail | Full control, plus privacy and reconciliation duties | Reasons, timestamps, and source identifiers survive export |
| Capacity | Service limits and external queues bound bursts | Compute, network, queue, and reputation capacity belong to the team | Recovery traffic retains reserved capacity during order load |

Capacity planning should use the marketplace's forecast peak, not an invented industry number. Model recovery requests and seller-order notices as separate arrival streams, include retry amplification, and verify that queue consumers and event ingestion can drain the resulting backlog before reset tokens expire. The SLO question is blunt: under the forecast order surge, does recovery keep enough reserved throughput to remain inside its completion objective?

**Prefer the smallest irreversible commitment.** Owning normalized evidence and recovery policy improves portability, but self-hosting the entire mail path is a poor bargain when the team cannot staff abuse response and reputation operations. Conversely, a managed sender saves integration and on-call effort only if suppression exports, event replay, authentication controls, and replacement tests meet the contract.

This contract-first approach has limitations. It is not suitable for a team that cannot own an event ledger, privacy controls, replay tooling, and regular adapter tests; that team should choose a managed transport with adequate exports and accept the provider-specific boundary. The opposite trade-off also matters: a regulated or isolated deployment may require self-operation despite its larger on-call burden. Neither route removes the need to authenticate the domain or handle bounces correctly.

## Verify, release, and roll back

Build the test suite before the cutover. It needs fixtures for acceptance, temporary failure, permanent failure, complaint, duplicate event, and out-of-order event. Six fixtures are enough to establish the minimum state-machine coverage described here, although they say nothing about inbox placement. Replay each fixture and verify that an older event cannot overwrite a newer terminal disposition.

Next, send through the production queue to controlled inboxes and inspect headers. Then introduce marketplace load that matches the planning model while checking queue age by stream, request-to-accept latency, event lag, suppression transitions, and reset completion. Seller-order volume is useful as competing load; it is not a proxy for recovery success.

Release by recipient-domain cohort or another observable segment rather than by an untraceable global trickle. Keep one variable per change window. Moving transport, rotating the DKIM selector, changing the return path, and editing the template together produces an incident with four plausible causes.

Rollback is a routing action. Stop increasing the new cohort, send new attempts through the last known-good path, and continue consuming events from both transports so late permanent failures and complaints still reach the suppression ledger. Do not resend an accepted attempt merely because its final disposition has not arrived, and never send it after token expiry.

The rollback checkpoint is concrete: new recovery attempts use the stable route, both event feeds remain active, suppression transitions reconcile without regression, queue age is back inside its objective, and completion has returned within the recovery SLO. Keep the evidence. The next provider decision should begin with these contract results, not another feature grid.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://www.rfc-editor.org/rfc/rfc7208
- https://www.rfc-editor.org/rfc/rfc6376
- https://www.rfc-editor.org/rfc/rfc7489
- https://www.ftc.gov/business-guidance/resources/can-spam-act-compliance-guide-business
