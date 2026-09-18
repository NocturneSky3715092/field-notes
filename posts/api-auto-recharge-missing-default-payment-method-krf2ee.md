# API Auto Recharge Missing Default Payment Method Explained — Why Service Stopped

Short answer: treat automatic recharge as a payment workflow with an explicit default-payment check, not as a promise that a balance will refill. In an edtech platform, record the balance transition, the payment-method identifier, and the decision that stopped or continued service. Alert before the final debit fails, with attribution for the learner account, ledger entry, and payment attempt.

The page that wakes an on-call engineer is blunt: a classroom API returns `402`, the prepaid pool is zero, and the dashboard says auto recharge is enabled. The useful question is narrower: which payment method did the recharge attempt resolve to, and can that answer be proven from an immutable event?

## Why did the service stop even though API auto recharge was configured?

Start at the stopped service, then work backwards. Capture account ID, ledger version, last successful debit, recharge threshold, and payment-attempt ID. `auto_recharge=true` is configuration, not evidence of a charge.

In a representative flow, a tutoring account has 18 credits, a threshold of 20, and a check every five minutes. The check sees the threshold, but no default payment method exists. The correct outcome is `missing_default_payment_method`; the worker must not select an old card, retry forever, or mark the balance funded. Enter a deliberate grace state with a visible deadline instead.

The alert should contain `account_id`, `ledger_version`, `recharge_attempt_id`, nullable `payment_method_id`, `reason_code`, and `observed_at`. Redact secrets and token material. OWASP recommends centralized secret handling, limited exposure, and audited access rather than credentials in logs or source files.

It fails closed.

## Why can a configured recharge still stop service?

Configuration, payment-instrument resolution, and ledger settlement are separate state machines. A method may be pending verification, detached after configuration, or hidden by a stale projection. One boolean cannot represent these states.

```go
type RechargeDecision struct {
	AccountID string
	LedgerVersion int64
	AttemptID string
	PaymentMethodID *string
	ReasonCode string
	AmountCredits int64
	ObservedAt time.Time
}

func decideRecharge(balance, threshold int64, methodID *string) RechargeDecision {
	d := RechargeDecision{AmountCredits: threshold - balance, ObservedAt: time.Now().UTC()}
	if balance >= threshold { d.ReasonCode = "threshold_not_reached"; return d }
	if methodID == nil || *methodID == "" { d.ReasonCode = "missing_default_payment_method"; return d }
	d.PaymentMethodID = methodID
	d.ReasonCode = "ready_for_payment_attempt"
	return d
}
```

The function does not charge anything. A separate adapter can turn `ready_for_payment_attempt` into an idempotent request while the ledger records the result. A pure decision is easy to replay and prevents a retry from creating a second credit grant.

That separation also makes the awkward case diagnosable: if a webhook arrives after the lesson service has already entered grace, reconciliation can compare the attempt ID and ledger version without guessing which card was intended, while an operator can see whether the missing method was a real account state or a stale projection. The extra event is cheaper than reconstructing intent from provider logs during an incident.

The signal should have fired earlier.

Page on recharge eligibility lacking an attributable payment method beyond the grace window, not only after service stops. Warn when the balance is below two lesson sessions and include projected exhaustion time. Thresholds belong to the product SLO, but the rule must use observed debits, not a vendor setting.

False positives train on-call engineers to ignore the next page. Join each alert to a later ledger event: if a method is attached within the grace window, resolve without charging; otherwise stop new work predictably and preserve the last successful ledger version.

Capacity planning matters. A five-minute poll over 200,000 accounts is 40,000 evaluations per minute before retries, and an outage can multiply that load. Use bounded concurrency, exponential backoff, and an idempotency key derived from account ID plus ledger version. Keep retry workers from competing with lesson traffic for the same database pool.

## How should teams choose the operating boundary?

Managed payment execution reduces card-handling surface area; a self-hosted ledger gives stronger control over attribution and replay. The decision is an SRE trade-off.

| Boundary | Strength | Cost or risk |
| --- | --- | --- |
| Managed payment execution | Less credential exposure | Provider states need translation |
| Self-hosted decision and ledger | Deterministic replay and portable data | More on-call work for retries and reconciliation |
| Split model | Payment data stays isolated | Two systems must agree on idempotency and clocks |

Set an SLO such as “99.9% of eligible recharges have a recorded decision within five minutes,” then test missing methods, delayed webhooks, duplicate delivery, and ledger contention. A green configuration flag is not an SLO measurement.

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://datatracker.ietf.org/doc/html/rfc7231
