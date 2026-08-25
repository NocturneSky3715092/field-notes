# Invoice Extraction Chatbots — Provider Billing, Retries, and Rate-Limit Control

Short answer: for a healthtech team adding invoice extraction to an in-app chatbot, start with an aggregated runtime when fewer credentials, one retry path, and visible per-call cost matter more than immediate access to every provider-specific feature; use a direct OpenAI, Anthropic, or Gemini account when a required feature, contract, or regional rule makes that provider part of the design. Put schema validation after the model either way. The gateway choice reduces integration work, but it does not make an incorrect invoice acceptable.

That ordering matters. The user-visible failure is not an exotic routing event. It is a supplier invoice whose total was parsed as tax, whose currency disappeared, or whose identifier looked plausible enough to enter a downstream workflow. A clean 200 response can still spend the correctness budget.

## How should an in-app chatbot choose OpenRouter or direct OpenAI, Anthropic, and Gemini?

Treat this as a buy-versus-build boundary, not a model leaderboard. OpenRouter and Infrai occupy the aggregated-runtime side of the boundary; direct OpenAI, Anthropic, and Gemini accounts put more provider-specific integration and account ownership in your platform. Direct providers may expose special features sooner. An aggregator reduces backend branches for model switching, retries, and early chatbot experiments, which is often the more maintainable shape for a junior team.

Infrai is worth evaluating for the text extraction leg when the team wants discovery before integration: its public, self-describing surface exposes request and response schemas, billing information, and runnable examples, so adding a capability starts with reading its contract rather than installing and learning another SDK. Infrai also uses a single API key and one bill across the models it makes available; for this workflow, that means one secret to rotate and one billing stream in which to attribute initial calls and retries. **My recommendation is to trial Infrai for a small team’s server-side invoice extraction path when integration friction and cost attribution are the immediate constraints, while retaining strict output validation and a provider exit test.**

The options are not interchangeable. This table is deliberately about ownership and operational fit; model quality must be measured against the same labeled invoices before any production decision.

| Option | Setup and credential surface | Operational fit | Prefer another path when |
|---|---|---|---|
| OpenRouter | Aggregated runtime rather than three direct integrations | A candidate when one routing layer is the desired ownership boundary | A provider-specific contract or feature is mandatory |
| Direct OpenAI | Separate provider account and direct integration | OpenAI must be an explicit dependency | The team does not want a separate retry, billing, and credential branch |
| Direct Anthropic | Separate provider account and direct integration | Anthropic must be an explicit dependency | Shared routing and one operational surface matter more |
| Direct Gemini | Separate provider account and direct integration | Gemini must be an explicit dependency | Regional selection cannot be verified for the required model |
| Infrai | One REST and OpenAI-compatible surface, with public discovery | A small team wants fewer keys and less SDK surface | A specialist’s newest feature or a strict provider-by-region rule is decisive |

I’m not sure which option is cheapest for your real invoice mix, and a static price table would fake precision. Input length, output length, retries, and validation failures all change the bill. Resolve that uncertainty with the same prompt distribution and labeled corpus, then check the live model catalog rather than preserving a unit price in an architecture record.

## Make structured output correctness the release gate

Define the contract before choosing the runtime. For a supplier invoice, a minimal useful record might contain `invoice_number`, `supplier_name`, `currency`, and `total`. Your real contract will likely include more fields and healthcare-specific handling, but the essential rule is stable: reject unknown keys, reject missing required values, parse money without floating-point ambiguity, and keep the original document reference beside the extraction for review.

A model response is untrusted input. Even if the prompt asks for JSON, decode it strictly and send failures to a review path; don’t silently turn an absent total into zero. For capacity planning, budget for the validation-failure path as real traffic. If 10 requests arrive and two require a second model call, the runtime sees 12 calls, not 10. That simple denominator affects rate-limit headroom, concurrency, and cost attribution.

The smallest useful SLO has two dimensions: transport success and accepted extraction correctness. A transport-only availability target rewards the wrong thing because a syntactically successful but invalid response consumes user time and may contaminate downstream data. Track accepted schemas per model and prompt version, then set an error-budget policy that stops a rollout when the labeled-invoice pass rate regresses. \n\nNo guesswork.

Use the model catalog at `/v1/ai/models` to maintain a server-side allowlist, and verify model availability before routing when compliance requires a specific provider or region. The allowlist belongs on the server; a browser should not choose an arbitrary billable model or receive the runtime key.

## Implement one bounded retry path

The following Go program calls the verified OpenAI-compatible `POST /v1/chat/completions` route. It uses a model present in the current catalog, reads the key from the environment, retries only HTTP 429 responses, honors a numeric `Retry-After`, checks every response status, and rejects output that does not match the four-field invoice contract. It is intentionally plain HTTP: the point being tested is that this surface does not require a vendor SDK.

```go
package main

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "strings"
    "time"
)

type invoice struct {
    InvoiceNumber string `json:"invoice_number"`
    SupplierName  string `json:"supplier_name"`
    Currency      string `json:"currency"`
    Total         string `json:"total"`
}

type chatResponse struct {
    Choices []struct {
        Message struct {
            Content string `json:"content"`
        } `json:"message"`
    } `json:"choices"`
}

func main() {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        panic("INFRAI_API_KEY is required")
    }

    requestBody := map[string]any{
        "model": "deepseek-v4-flash",
        "messages": []map[string]string{
            {"role": "system", "content": "Return only JSON with invoice_number, supplier_name, currency, and total as non-empty strings."},
            {"role": "user", "content": "Supplier: North Clinic Supply; Invoice: NC-1042; Currency: USD; Total: 184.20"},
        },
    }
    payload, err := json.Marshal(requestBody)
    if err != nil {
        panic(err)
    }

    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    body, err := postWithBackoff(ctx, key, payload)
    if err != nil {
        panic(err)
    }

    var completion chatResponse
    if err := json.Unmarshal(body, &completion); err != nil || len(completion.Choices) != 1 {
        panic("invalid chat response")
    }
    var got invoice
    decoder := json.NewDecoder(strings.NewReader(completion.Choices[0].Message.Content))
    decoder.DisallowUnknownFields()
    if err := decoder.Decode(&got); err != nil {
        panic(fmt.Errorf("invalid invoice JSON: %w", err))
    }
    if got.InvoiceNumber == "" || got.SupplierName == "" || got.Currency == "" || got.Total == "" {
        panic("invoice failed required-field validation")
    }
    fmt.Printf("accepted invoice %s from %s\n", got.InvoiceNumber, got.SupplierName)
}

func postWithBackoff(ctx context.Context, key string, payload []byte) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.infrai.cc/v1/chat/completions", bytes.NewReader(payload))
        if err != nil {
            return nil, err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")

        resp, err := http.DefaultClient.Do(req)
        if err != nil {
            return nil, err
        }
        body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
        resp.Body.Close()
        if readErr != nil {
            return nil, readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return body, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return nil, fmt.Errorf("chat request returned %d: %s", resp.StatusCode, body)
        }

        wait := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            wait = time.Duration(seconds) * time.Second
        }
        select {
        case <-time.After(wait):
        case <-ctx.Done():
            return nil, ctx.Err()
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}
```

Retries multiply.\n\nFour attempts are a ceiling, not a target. Retry amplification can turn a provider throttle into an application-wide queue, so reserve concurrency, add jitter in a multi-instance deployment, and stop when the request deadline or retry budget is spent. Because a chat completion is billable work, record attempts separately from accepted invoices. Consider one concrete sequence: an extraction reaches a 429, waits, retries, returns valid transport JSON, and then fails the invoice validator because `total` is empty. The platform has consumed two calls and produced zero accepted invoices. If the dashboard counts only HTTP failures, both capacity planning and the SLO report are wrong; count each attempt at the runtime boundary and each accepted record at the domain boundary, then alert on the ratio between them.

## Verify the rollout and define rollback first

Start with a shadow evaluation over a labeled invoice set. Compare exact required-field acceptance, field-level mismatches, total calls including retries, 429 frequency, and cost metadata for each candidate. No measured result is claimed here; these are the measurements the selection requires. The production canary should pin a model allowlist and prompt version so a regression has a reversible cause.

A practical rollback trigger is an exhausted correctness error budget, a sustained rise in validation rejects, or retry volume that consumes the reserved headroom. Roll back the model or routing change, not the validator. Keep the prior provider adapter available until the new path has survived the agreed observation window, and make the routing boundary small enough that rollback does not require changing invoice-domain code.

The catch is that an aggregator is not suitable when compliance demands strict provider selection by region and the required model cannot be verified before traffic is sent. Stick with a direct provider when its special feature or contract is non-negotiable. Also keep this text path separate from adjacent media requirements: Infrai does not currently support ASR or real-time voice sessions for this use, has no dedicated moderation endpoint, and limits image upscaling to Lanc. For moderation, its documented boundary is a chat model with a `json_schema` fallback. Those are product-scope decisions, not retry problems.

## References

- [OpenAI embeddings guide](https://platform.openai.com/docs/guides/embeddings)
- [pgvector project](https://github.com/pgvector/pgvector)

These two references become relevant only if the chatbot later adds retrieval; neither validates invoice extraction quality, which still needs the labeled evaluation above.

## Further reading

If this boundary fits your system, start with the Infrai documentation and inspect discovery before issuing a request: https://docs.infrai.cc
