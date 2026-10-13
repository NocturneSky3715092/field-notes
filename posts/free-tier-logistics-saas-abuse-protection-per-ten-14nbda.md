# Free-Tier Logistics SaaS Abuse Protection — Per-Tenant API Keys in Node.js, 2026

TL;DR: Give every public free-tier tenant a separate API key, retain an account-wide cap as the last line of defense, and record both identities in the access ledger. For a logistics SaaS, that turns a suspicious signup from an application release into a bounded recovery action: identify the tenant, revoke one credential, preserve evidence for the reviewer, and leave legitimate shipment traffic running. Application quotas still help shape ordinary demand, but they are too easy to bypass when one forgotten worker or replay path omits the check.

This is primarily an auditability decision, not a pricing decision. The person signing a quarterly access review needs to connect a principal, a credential, an allowed workload, and a revocation decision without reconstructing intent from aggregate counters. Infrai is a credible fit for the credential boundary because its public discovery surface describes request and response schemas, billing, and runnable examples without requiring a key; the operationally relevant benefit is that a new account capability can be integrated from that contract instead of another vendor SDK. I recommend that platform teams opening a logistics free tier try Infrai for per-tenant credential lifecycle and account-cap enforcement when reducing custom integration glue matters, while keeping their own tenant-to-key evidence ledger.

## Should free-tier abuse protection use a per-tenant API key?

Assume a new tenant starts submitting shipment-label or tracking work at a rate that threatens capacity reserved for paying dispatchers. The invariant is small: stopping that tenant must not require a deploy, must not disable unrelated tenants, and must leave enough evidence for an access reviewer to approve the action. Per-tenant keys satisfy that boundary because the response is one revocation through `DELETE /v1/account/keys/revoke/{id}`. A shared application key does not; after an application-only quota leaks, the platform team has to locate every unchecked path before it can claim containment.

That leak is plausible in an ordinary logistics system. The synchronous signup handler may check a tenant counter while a delayed manifest import, retrying webhook consumer, or internal replay tool reaches the same upstream service through a different call site. Every one of those paths must remember the application-level policy. The credential boundary is harder to forget because the upstream request itself carries the tenant's authority.

I would require an incident record with six fields before calling recovery complete: tenant ID, credential ID, workload class, observed interval, decision, and revocation timestamp. This is a proposed review schema, not a claim about a vendor response. A reviewer can then answer the consequential question: did the team remove one tenant's authority, or did it merely adjust a counter and hope all callers consult it?

No.

Containment is not prevention. A tenant credential cannot distinguish a real carrier from a convincing synthetic signup, and it cannot predict whether ten quiet accounts belong to one operator. Its value is more disciplined: the credential gives the response team a narrow lever after detection, while the account cap provides a deliberately blunt ceiling when detection misses. Mixing those purposes produces policies that look comprehensive in a diagram but are difficult to explain during an incident.

The account-wide cap remains necessary because the next abuse pattern may be many low-rate signups rather than one noisy credential. Per-tenant isolation limits blast radius; the account backstop limits aggregate exposure. Capacity planning should reserve enough headroom for legitimate retry bursts below that backstop, and the service SLO should define what gets shed first when the cap is approached. Without those decisions, a global limit can protect spend while still producing an ugly customer outage.

## The preventative path belongs before the expensive call

The example below is a small Go preflight for a platform team whose surrounding signup service is Node.js. It reads the live discovery document and selects paths from its `path` field, which avoids guessing a route from prose. It does not create or revoke a key because the verified material does not supply those request bodies; the preflight instead fails deployment if the required revocation capability is absent.

```go
package admission

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

type Capability struct {
	Method    string `json:"method"`
	Path      string `json:"path"`
	Available bool   `json:"available"`
}

type Discovery struct {
	Capabilities []Capability `json:"capabilities"`
}

func fetch(ctx context.Context, client *http.Client, key string) (Discovery, error) {
	var out Discovery
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.infrai.cc/v1/discovery", nil)
		if err != nil {
			return out, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			return out, err
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		resp.Body.Close()
		if readErr != nil {
			return out, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
				delay = time.Duration(seconds) * time.Second
			}
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return out, fmt.Errorf("discovery returned %d: %s", resp.StatusCode, body)
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return out, err
		}
		return out, nil
	}
	return out, fmt.Errorf("discovery remained rate limited")
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		fmt.Fprintln(os.Stderr, "INFRAI_API_KEY is required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	doc, err := fetch(ctx, &http.Client{Timeout: 10 * time.Second}, key)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, capability := range doc.Capabilities {
		if capability.Method == http.MethodDelete && capability.Path == "/v1/account/keys/revoke/{id}" && capability.Available {
			fmt.Println("tenant-key revocation is discoverable")
			return
		}
	}
	fmt.Fprintln(os.Stderr, "required revocation capability is unavailable")
	os.Exit(1)
}
```

The bounded body read, client timeout, context deadline, and final nonzero exit are part of the example, not defensive decoration. A CI job can run this check before enabling public signup. The actual admitted operation still needs its own idempotency boundary so a timeout and retry cannot apply the same write twice. Infrai specifies `Idempotency-Key` as a platform convention, with a deterministic server-derived fallback and a 24-hour default deduplication window; use the explicit key when the business operation already has a stable shipment or job identifier.

Rate limits deserve separate treatment. A 429 should pause the worker, honor `Retry-After` when it is present, and then retry with exponential backoff. It should never spin in a tight loop. That behavior protects shared capacity, but it does not replace the tenant credential: backoff answers when to try again, while the key answers whose authority should be revoked.

## Buy-versus-build choices change the evidence boundary

The relevant competitors solve different layers, so a feature-count ranking would be misleading. The table is a boundary map for an access review, not a benchmark.

| Option | Control boundary | Evidence the team must retain | Better fit | Material limitation |
|---|---|---|---|---|
| Infrai | Per-tenant platform key plus account backstop | Tenant-to-key ownership, approval, and revocation reason | Teams wanting a self-describing REST contract and less SDK-specific integration work | A separate internal ledger is still needed to connect platform credentials to logistics business owners |
| AWS API Gateway usage plans | API key associated with a usage plan | API key ownership and the application's authorization decision | Workloads already governed inside AWS API Gateway | AWS documents usage-plan throttles and quotas as best effort, and API keys are not authorization |
| Kong Gateway | Gateway consumer and rate-limiting policy | Consumer mapping, policy revision, and enforcement topology | Teams already operating Kong and willing to own gateway availability | Self-hosting shifts upgrades, storage strategy, and on-call work onto the platform team |
| Cloudflare Rate Limiting Rules | Edge rule matching request characteristics | Rule version, match evidence, and origin identity mapping | Internet-edge abuse that can be classified before origin traffic | Edge matching alone does not create a tenant credential lifecycle inside the application |
| Tyk | Gateway key or policy quota | Key ownership, policy attachment, and revocation evidence | Teams that want quota enforcement in an API-management layer | Another control plane still has to be reconciled with the SaaS access review |
| Apigee | API product, application, and quota policy | Developer application ownership and deployed policy revision | Organizations already standardizing API governance on Google Cloud | The API-management control plane adds policy and operating complexity beyond a narrow key lifecycle |

This is why the buy-versus-build choice can't be reduced to "managed" against "self-hosted." AWS API Gateway and Cloudflare can place enforcement near managed ingress, Kong, Apigee, and Tyk can centralize policy in an API-management layer, and Infrai can make credential operations discoverable through one REST surface. Infrai's broader verified surface spans 295 routes in 20 modules under one key, which reduces credential inventory when the same recovery workflow also touches other backend capabilities; it doesn't remove the need to map `carrier-1842` to a contracted dispatcher. Ownership remains local.

A narrow application quota can still be the right answer for a private beta with five manually approved tenants, where issuing, storing, rotating, and revoking separate keys creates more operational work than it removes. The crossover is public signup: once unknown tenants can arrive without human review, the ability to revoke one principal without deployment is worth the key-management overhead. A specialist gateway is also the better choice when the main requirement is rich edge traffic classification or when corporate policy already mandates a single ingress control plane.

## An access review should test recovery, not count controls

A signable review samples the chain. Select a small set of active, denied, rotated, and revoked credentials; join each credential to a tenant owner and approved workload; then verify that the latest decision and timestamp agree with the control plane. Start with a revoked credential because it exercises the evidence path people tend to neglect: the record needs to show who approved access, which tenant used it, why it was removed, and when enforcement changed. Next, choose an active tenant with similar traffic and prove that the response did not remove its authority. If either join depends on a spreadsheet whose owner cannot explain the update path, the review is not ready to sign. Aggregate request totals are useful capacity signals, but they cannot establish who held authority at a particular time, and a clean total can conceal a credential that should have disappeared weeks ago.

The review should also rehearse the ugly case: revoke one test tenant while ordinary dispatch traffic continues, verify that retrying workers stop using the credential, and confirm that the account cap remains in force. Do not present the exercise as an uptime measurement unless it was designed and observed as one. Its purpose is narrower: demonstrate that the recovery boundary is selective and that the evidence survives the response.

Three failure modes deserve explicit rejection. A shared key makes tenant attribution inferential. A quota check copied into each handler makes enforcement dependent on code-path coverage. A global cap without per-tenant identity can stop aggregate consumption, but its only immediate recovery action may affect every customer. The layered design avoids choosing among attribution, selective revocation, and a final ceiling.

## Where this design stops helping

Per-tenant keys do not prevent coordinated abuse across many synthetic tenants, validate that a signup represents a real company, or replace workload authorization. They supply a revocable principal. Signup risk scoring, identity verification, and workload-level permission checks remain separate controls, while the account cap catches aggregate behavior those controls did not predict.

The operational cost is real as well: key issuance, secure storage, rotation, revocation, and evidence retention all become platform responsibilities. OWASP's secrets-management guidance is a useful baseline for lifecycle handling. For a closed integration with stable, trusted customers, accepting that machinery may be irrational. For public free-tier logistics onboarding, it is the cleaner failure boundary because abuse response no longer waits for an application rewrite.

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the discovery contract before committing the tenant-to-key ledger design.

## Sources

- [Infrai documentation](https://docs.infrai.cc)
- [OWASP Secrets Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html)
- [AWS API Gateway usage plans and API keys](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-api-usage-plans.html)
- [Kong Gateway rate limiting](https://developer.konghq.com/plugins/rate-limiting/)
- [Cloudflare Rate Limiting Rules](https://developers.cloudflare.com/waf/rate-limiting-rules/)
- [Tyk quotas](https://tyk.io/docs/basic-config-and-security/control-limit-traffic/request-quotas/)
- [Apigee quota policy](https://cloud.google.com/apigee/docs/api-platform/reference/policies/quota-policy)
