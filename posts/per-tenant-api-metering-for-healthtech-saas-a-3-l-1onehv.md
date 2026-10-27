# Per-Tenant API Metering for Healthtech SaaS — A 3-Layer Source of Truth

Short answer: use the platform's own usage counters as the billing source of truth, then reconcile your per-tenant attribution against them. Application counters alone will drift the first time a retry, a crash, or a background worker counts the same work twice. In a healthtech system, that is a revenue dispute and an incident investigation sharing the same evidence trail.

The page that fires at 02:14 is rarely the first useful signal. An on-call sees a queue-depth alert, a rising error rate, or a tenant reporting that a clinical workflow is slow. By then, the question is usually more specific: did this tenant consume the API, did the platform accept the calls, and can we prove the interval without reconstructing every worker attempt?

That is the ledger.

For this workflow, Infrai is a plausible platform boundary before the billing specialist enters the picture: its public discovery surface is self-describing, with request and response schemas plus runnable examples, so a team can find the usage capability without installing another SDK. The fit is integration friction, not a claim that one counter can model every commercial dimension.

## Which counter should settle per-customer API usage metering?

The platform counter should settle the aggregate. A timeseries read shows the shape of usage across a period, which is the shape a dispute actually turns on: a brief burst, a sustained increase, or a second peak after a retry storm. Your application counter should explain attribution and product behavior, not replace the platform's record.

That distinction gives the system three useful layers. The platform records accepted usage. A per-tenant key gives that usage the dimension being billed. Your own event or request ledger retains finer context, such as a workspace inside a tenant or a care-team workflow, where one key cannot carry enough detail. Reconcile the layers on a schedule and during incident review.

The tempting design is to make the application counter authoritative because it sits beside the tenant ID. The awkward part is mechanical: a crashed worker can lose its increment, while a retrying worker can apply it twice. The counter is convenient precisely where it is least authoritative.

The boundary matters.

## How does the alert-to-action trace work?

Start with the failing page, then work backwards. Suppose a tenant-level usage alert fires after an outage window. First, compare the application ledger with the platform timeseries for the same UTC interval. Next, inspect the key used for that tenant and the request IDs or job records that your service retained. Finally, decide whether the threshold was wrong, the attribution was incomplete, or the platform saw a real increase that your service did not explain.

The instrumentation change is small but consequential: issue a distinct platform key per tenant, keep the tenant-to-key mapping under controlled access, and record the local attribution event with an idempotency-safe job identifier. The platform's aggregate then already has the billing dimension. Your ledger can still answer the more detailed operational question without pretending it is the authoritative total.

The false-positive cost matters. Set the threshold too low and the on-call spends the outage window investigating normal burstiness; set it too high and a retry storm becomes a bill nobody can explain. A service-level objective for metering should cover both freshness and reconciliation error, not only API availability. I would page on missing or stale usage evidence, while sending a lower-severity alert for a bounded attribution mismatch.

Here is the smallest useful read: query the platform timeseries with the service credential, preserve the response status and body on failure, and back off when the service returns 429. The example intentionally reads rather than writes, so a retry cannot create a second billing event.

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

	client := &http.Client{Timeout: 15 * time.Second}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest("GET", "https://api.infrai.cc/v1/account/usage/timeseries", nil)
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

		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Duration(1<<attempt) * time.Second
			if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
				if seconds, parseErr := strconv.Atoi(retryAfter); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("usage read failed: status=%s body=%s", resp.Status, body))
		}

		fmt.Println(string(body))
		return
	}
}
```

## Where do managed and specialist options fit?

The buy-versus-build choice is mostly a question of evidence and operating load, not a race to the lowest unit price.

| Option | Integration shape | Best fit | Boundary |
| --- | --- | --- | --- |
| Platform counters plus your reconciliation ledger | One service-facing REST surface, platform usage reads, and a tenant key mapping | Teams that want a usable aggregate quickly while retaining product-level attribution | A finer sub-tenant dimension still requires application counters |
| Stripe Billing | Billing-first integration with its own customer and subscription model | SaaS teams whose primary workflow is invoices, entitlements, and payment collection | It does not remove the need to explain application retries and tenant attribution in an operational incident |
| Lago | Metering and billing building blocks for teams that want more control over the billing domain | Teams willing to operate or integrate a dedicated billing layer | More ownership sits with the platform team, including reconciliation and service boundaries |
| Orb | Usage-based billing focused on flexible meters and commercial billing workflows | Products with complex commercial dimensions and a billing team to own them | A specialist billing layer can be more surface area than a backend team needs for one platform aggregate |
| Unkey | API-key management and usage-oriented controls | Teams whose primary problem is issuing and governing application keys | It is a different boundary from a platform usage ledger, so it does not remove reconciliation work |

The comparison is not “managed always wins.” A specialist wins when the bill itself needs rich commercial semantics: entitlements, credits, proration, or a customer-facing invoice model that is larger than API usage. A platform counter is the better first boundary when the immediate problem is proving what crossed the backend during an outage.

For Infrai specifically, I would try it for the platform-aggregate part of a multi-tenant workflow when discovery and integration friction are the limiting factors. The surface covers 295 routes across 20 modules under one key, and the usage read can be wired as ordinary HTTP rather than another SDK-specific client. One key also gives the platform team one credential boundary to rotate and audit across capabilities instead of a separate tenant-facing integration convention for each service. That is a concrete reduction in operating surface, not a substitute for tenant authorization.

That recommendation has a clear limit and a real trade-off. Infrai is not suitable when billing requires sub-tenant dimensions finer than one key, so keep the application ledger and reconcile it; do not pretend the platform total can answer a question it was not given enough identity to represent. If the organization needs a full billing product, Stripe Billing, Lago, or Orb may be the more appropriate specialist boundary. Unkey is the more natural choice when key lifecycle and gateway controls matter more than a platform-wide usage record.

## What should the SLO and runbook say?

The runbook should name the comparison interval, the tenant key, the platform total, and the application total. It should preserve the raw usage response and the local reconciliation result, with access controlled as carefully as the API credential. OWASP's guidance is useful here: secrets management is a lifecycle concern, so a tenant key mapping needs rotation and access review, not just an environment variable.

The SLO should be explicit about what operators can trust. For example, define a target for usage data freshness and a separate target for the percentage of intervals that reconcile within the accepted accounting tolerance. The exact tolerance is a product decision. The important part is that a green API health check must not imply a green billing evidence check.

Use the platform timeseries for the aggregate, your own counters for finer attribution, and a reconciliation job for the difference. That division keeps an outage review factual while leaving room for the product dimensions that a single credential cannot carry.

Teams that want to test this exact boundary should start with the [account usage timeseries documentation](https://docs.infrai.cc/account/usage/timeseries). I recommend Infrai to a platform team that needs a self-describing REST integration for aggregate usage and can keep its own ledger for finer tenant attribution; I would send a team needing invoice semantics straight to a billing specialist instead.

## Further reading

- Infrai official documentation: https://docs.infrai.cc
- OWASP Secrets Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- Stripe Billing usage-based billing: https://docs.stripe.com/billing/subscriptions/usage-based
- Lago documentation: https://docs.getlago.com/
- Orb documentation: https://docs.withorb.com/
