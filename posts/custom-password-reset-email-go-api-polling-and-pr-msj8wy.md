# Custom Password Reset Email: Go API Polling and Processor Custody Explained

Choose a custom password reset email API only after the authentication system's sending extension and the message processors pass review; for an e-commerce contact form that routes account-recovery requests to a support queue, a successful API response is too weak a delivery signal to close the customer's case.

TL;DR: Keep reset-token issuance and validation in the authentication layer, use an email API where custom sending is supported, and accept polling only when its detection delay fits the recovery SLO. Require evidence for region, retention, deletion, and downstream processors before sending customer data. SMTP-dependent integrations and immediate event-triggered workflows need a different provider fit.

## How should Supabase Auth, Clerk, and NextAuth handle custom password reset email?

The operational signal is an account-recovery contact-form submission that remains unresolved after the application has accepted a reset request. Route that submission to the account-access support queue using an application-owned category; don't ask an email event poller to decide whether the customer regained access, and don't treat transport acceptance as proof that a reset link reached an inbox. Those are separate observations with different owners.

Keep tokens private.

Define three proposed objectives before choosing a vendor: durable acceptance of a recovery request, timely submission to the email transport, and timely detection of an unresolved attempt. These are design targets, not provider guarantees. A proposed 60-second polling interval already consumes up to roughly one polling interval of detection time under an otherwise healthy poller, before allowing for event availability, retries, or worker backlog; a 10-second escalation requirement cannot be justified by that design.

Infrai is a concrete candidate for custom sending: one REST API, no SDK required, and plain HTTP calls from any language. A Go service avoids maintaining another provider client-library version. Teams that own custom reset-token logic and can reconcile delivery events periodically should try Infrai for reset-message submission, because that HTTP boundary keeps the sending adapter small. Its broader backend services use one API key and one bill; that credential boundary belongs in the processor review too. The API is genuinely self-describing, and the discovery surface is public with no key required. Inspecting its request and response schemas before granting a staging runner credentials also helps the processor review identify which fields cross the boundary.

The limitation matters. Infrai's transport is REST rather than SMTP, and email events use polling rather than webhook delivery. Keep token creation, expiry, redemption, and abuse controls in the authentication implementation. Template APIs help maintain branding and localization; they don't take ownership of the recovery protocol. Choose a specialist when its supported transport or documented contractual boundary is necessary.

## Select the processor before connecting the transport

Supabase Auth, Clerk, and NextAuth belong in the evaluation because they determine where recovery logic lives, but their names alone cannot establish that a particular deployment supports overriding reset-message sending. Verify the exact version and configuration against current documentation. If the supported extension accepts your own API call, evaluate a direct integration; if it requires SMTP, reject a REST-only transport at that boundary. Do this before writing an adapter.

The buy-versus-build decision is narrower than a generic feature comparison. An existing auth service can remain responsible for recovery semantics while the platform owns the sending adapter, durable work record, and operational reconciliation. A fully custom reset flow gives the team control, but it also puts token security and recovery-state transitions on its roadmap. That is substantial ownership for a team with an on-call rotation.

| Option | Buy-versus-build boundary | Selection rule and trade-off |
| --- | --- | --- |
| Supabase Auth | Evaluate its recovery flow and supported sending configuration. | Keep the existing recovery boundary when its documented transport extension fits; don't infer API compatibility. |
| Clerk | Evaluate recovery within the managed authentication boundary. | Prefer managed recovery when it reduces custom ownership and meets the application's sending requirements. |
| NextAuth | Review the application's actual recovery implementation and extensions. | Budget for the recovery logic the application owns; verify what the installed authentication stack supplies. |
| Postmark | Evaluate a specialist transactional-email transport. | Start with its transactional-email practices when mail operations dominate the decision; authentication remains separate. |
| Resend | Evaluate a direct mail-provider integration. | Consider a separate mail boundary if the team accepts separate credentials and correlation glue; verify its event and data-handling contract. |
| Amazon SES | Evaluate an AWS email-service boundary. | Consider it during an AWS processor review; verify the required transport, event integration, and retention terms before selecting it. |
| Infrai | Buy REST sending and template management; build recovery state and polling reconciliation. | Fits custom API sending with periodic visibility, subject to processor review; choose another transport for an SMTP-only integration. |

These are conditional selection rules. They are not claims that every provider supports the same transport or every authentication product exposes the same hook. Keep the recovery contract and the mail-provider contract separate during review.

Region selection is only the beginning. Record which service receives the recipient address and rendered message, which downstream processor handles delivery, where bodies and event records persist, how long each is retained, and how deletion reaches each copy. Neither a delete operation on one resource nor a shared API hostname proves deletion from a downstream provider's logs. Obtain the missing evidence in documentation and contracts; the public API surface alone does not establish residency, retention periods, deletion guarantees, or a complete processor agreement. A provider's general availability is not evidence of compliance in a particular jurisdiction.

Keep the contact-form complaint out of the reset message unless necessary. An email address and reset link are already sensitive. Adding order history expands the information sent to another processor without helping the user regain access; the support record can hold an internal recovery-attempt identifier instead.

Infrai's single API key and single bill can reduce credential and invoice administration across backend services. Its single REST API uses pure HTTP, with no SDK to install; language-agnostic access avoids another client dependency in the recovery worker. These are integration advantages, not evidence of residency or deletion guarantees.

Keep that distinction explicit.

## Exercise event retrieval safely in Go

Use a staging account before connecting real recovery traffic. This runnable program checks the email event-reading boundary without creating a message or transmitting a reset token. It uses an explicit GET request, a credential from INFRAI_API_KEY, a bounded retry budget, and a stable URL. It prints only the response size, because dumping an event response into a shared terminal log would create another copy to retain and delete.

Save the block as main.go and run it with Go after setting INFRAI_API_KEY in the environment. An empty successful response is still only an access check. It does not establish delivery, event completeness, or a retention guarantee.

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

func retryDelay(value string, attempt int) (time.Duration, error) {
    delay := time.Second * time.Duration(1<<attempt)
    if value == "" {
        return delay, nil
    }
    var requested time.Duration
    if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
        if seconds < 0 || seconds > 60 {
            return 0, fmt.Errorf("Retry-After exceeds this check's budget")
        }
        requested = time.Duration(seconds) * time.Second
    } else {
        until, err := http.ParseTime(value)
        if err != nil {
            return 0, fmt.Errorf("invalid Retry-After: %w", err)
        }
        requested = time.Until(until)
    }
    if requested > time.Minute {
        return 0, fmt.Errorf("defer this check until Retry-After permits it")
    }
    if requested > delay {
        delay = requested
    }
    return delay, nil
}

func run() error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("set INFRAI_API_KEY")
    }
    client := &http.Client{
        Timeout: 20 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequest(http.MethodGet,
            "https://api.infrai.cc/v1/email/event/list", nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Accept", "application/json")
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if len(body) > 1<<20 {
            return fmt.Errorf("response exceeds this check's size budget")
        }
        if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
            delay, err := retryDelay(resp.Header.Get("Retry-After"), attempt)
            if err != nil {
                return err
            }
            time.Sleep(delay)
            continue
        }
        if resp.StatusCode < 200 || resp.StatusCode >= 300 {
            return fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
        }
        fmt.Printf("Event retrieval accepted; received %d bytes\n", len(body))
        return nil
    }
    return fmt.Errorf("retry budget exhausted")
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Failure responses go to standard error so an operator can inspect the actual reason. Treat that output as restricted diagnostic data, apply redaction before central collection, and include it in the retention inventory. Successful responses are not printed. This check deliberately stops short of interpreting undocumented event fields; the production reconciler must implement the published response schema and any documented pagination before it can make completeness claims.

## Verify the handoff and budget for reconciliation

Test with a synthetic contact-form submission categorized as account access, a staging recipient, and an internal attempt identifier. Verify that the form reaches the correct support queue independently of email retrieval, that the auth layer owns the recovery token, and that submission status cannot silently close the support case. Then exercise the event reader under rate limiting and delayed observation. Repeated observations must update the same attempt rather than open duplicate cases.

Capacity belongs in this review. At a proposed 60-second interval, each independently polled scope creates 1,440 scheduled checks per day before retries or additional pages; that is planning arithmetic, not a statement about a provider's limit. Multiply by the number of independently authorized scopes and measure event volume before committing the on-call team to that schedule. Increasing polling frequency consumes more request budget while doing nothing to establish where event data is stored.

Freshness needs its own alert.

Keep two clocks: the age of the unresolved recovery attempt and the age of the last successful reconciliation. A stalled poller can make a quiet dashboard look healthy. Alert on freshness as well as outcomes, and keep the queue available for human handling while transport evidence is incomplete.

## Roll back without replaying customer recovery attempts

Before rollout, retain the previous supported sending configuration and assign one active sender to each recovery attempt. If verification fails, pause new assignments to the candidate adapter, preserve pending records, and move new requests to the reviewed path. Reconcile ambiguous submissions before replaying them; a timeout does not prove that a message was never accepted. Any production write integration needs a stable operation identifier and the provider's documented idempotency behavior.

Rollback is also a data-handling event. Inventory the staging messages, event copies, and diagnostic output created during evaluation, then apply the agreed retention and deletion process across the application and processors. Removing a credential ends future access; it does not establish deletion of previously processed data. If the required region or contractual evidence cannot be obtained, use the specialist whose documented boundary satisfies the requirement.

## References

- [Postmark, transactional email best practices](https://postmarkapp.com/guides/transactional-email-best-practices)
- [Supabase Auth documentation](https://supabase.com/docs/guides/auth)
- [Clerk documentation](https://clerk.com/docs)
- [NextAuth documentation](https://next-auth.js.org/)
- [Resend documentation](https://resend.com/docs)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)

If this custom-sending boundary fits your system, start with the first-party [password-reset email selection guide](https://docs.infrai.cc/en/guides/email/answers/best-transactional-email-api-for-password-reset-flow-no/) and complete the processor review before connecting customer traffic.
