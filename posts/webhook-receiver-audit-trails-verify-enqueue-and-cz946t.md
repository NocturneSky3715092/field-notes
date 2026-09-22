# Webhook Receiver Audit Trails: Verify, Enqueue, and Prove Who Read the Raw Body

Use this rule when you build a webhook receiver that will eventually show up in an access review: verify the signature over the exact raw bytes before anything parses them, enqueue those bytes, acknowledge fast, and write one audit record per request — accepted, rejected, or dropped — that a human can export without shell access to production.

The system I have in mind is edtech, and it is mundane. A student information system pushes roster changes and grade-passback events to a district intake service; twice a year someone has to sign a document stating which people and which services could read student records during the previous term. The receiver is usually the weakest paragraph in that document, and it isn't because the code is insecure.

Because nobody wrote down what it did.

## The failure mode that surfaces when someone has to sign

The classic one is a parsing accident. A JSON body parser is mounted globally, it consumes the request stream before the webhook handler runs, and the handler then computes an HMAC over a re-serialized object — which isn't the byte sequence the sender signed, because key order, unicode escaping, and whitespace all survive the wire and do not survive a round trip through a parser. Teams discover this the moment a sender emits a payload with a non-ASCII student name, at which point signature verification fails for exactly the records that matter most. On Node.js the fix has a name: mount `express.raw({ type: '*/*' })` on the webhook route and mount it before any JSON parser, so the handler owns a `Buffer` of the original bytes rather than a reconstruction of them.

The second failure is quieter and it is the one that sinks access reviews. The receiver logs the full payload at debug level "temporarily", the log pipeline retains it for 400 days, and the log system grants read access to everyone in the engineering group. You now have a copy of student data in a datastore whose access model nobody mapped, and the review has to describe it.

Third: acknowledging before the payload is durable. The handler writes to the database, then to a search index, then returns 200 after 4 seconds; the sender's client timed out at 3 and retried; you processed the event twice and logged it once.

The first access review I sat through went sideways on a question I could not answer in the room — which signing key was in force on the day a vendor's contractor lost a laptop. One shared secret in an environment variable, no key identifier, no rotation record, so the honest answer was that we couldn't tell. That answer converts a one-hour meeting into a three-week project, and it is the reason I now treat the key identifier and the audit record as load-bearing parts of the receiver rather than as logging garnish.

## How should a webhook receiver verify the signature over the raw body and still acknowledge fast?

Read the body once, with a size cap. Verify an HMAC computed over a signed string that includes a message id and a timestamp, so a captured request cannot be replayed a week later. Compare in constant time. Then enqueue the raw bytes under an idempotency key and return before you touch a database.

The Standard Webhooks convention is a reasonable default because it pins down the ambiguous parts: `webhook-id`, `webhook-timestamp`, and `webhook-signature`, with the signed content being the id, the timestamp, and the body joined by periods, and the signature header carrying one or more space-separated `v1,<base64>` values so that two keys can be valid during a rotation. That last property is what makes the audit record possible — if two keys can sign, the receiver learns which one actually did.

```go
package intake

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"
)

type Key struct {
	ID     string // recorded on every accepted request; this is what an access review asks for
	Secret []byte
}

// Active returns every key currently valid for a tenant, newest first.
// During a rotation it returns two; outside a rotation, one.
type Keyring interface {
	Active(tenant string) []Key
}

func verify(keys []Key, id, ts, header string, raw []byte, now time.Time, tolerance time.Duration) (string, error) {
	secs, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return "", errors.New("unparseable timestamp")
	}
	if delta := now.Sub(time.Unix(secs, 0)); delta > tolerance || delta < -tolerance {
		return "", errors.New("timestamp outside tolerance")
	}

	signed := append([]byte(id+"."+ts+"."), raw...)
	for _, k := range keys {
		mac := hmac.New(sha256.New, k.Secret)
		mac.Write(signed)
		want := base64.StdEncoding.EncodeToString(mac.Sum(nil))
		for _, part := range strings.Split(header, " ") {
			got, ok := strings.CutPrefix(part, "v1,")
			if ok && hmac.Equal([]byte(got), []byte(want)) {
				return k.ID, nil
			}
		}
	}
	return "", errors.New("no active key matched")
}
```

The handler around it stays boring on purpose. Every exit path produces an audit record, including the rejections, because a review that only lists successes cannot answer "was anything probing this endpoint in March".

```go
const maxBody = 256 << 10 // roster events are small; the cap is a cheap bound on memory per request

type AuditRecord struct {
	At         time.Time
	Tenant     string
	EventID    string
	KeyID      string
	BodySHA256 string // the digest, never the payload
	Outcome    string
}

func (r *Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	rec := AuditRecord{At: r.now(), Tenant: tenantFrom(req)}
	raw, err := io.ReadAll(http.MaxBytesReader(w, req.Body, maxBody))
	if err != nil {
		r.finish(w, rec, "rejected_oversize", http.StatusRequestEntityTooLarge)
		return
	}
	sum := sha256.Sum256(raw)
	rec.BodySHA256 = hex.EncodeToString(sum[:])
	rec.EventID = req.Header.Get("webhook-id")

	keyID, err := verify(r.keys.Active(rec.Tenant), rec.EventID,
		req.Header.Get("webhook-timestamp"), req.Header.Get("webhook-signature"),
		raw, r.now(), 5*time.Minute)
	if err != nil {
		r.finish(w, rec, "rejected_signature", http.StatusUnauthorized)
		return
	}
	rec.KeyID = keyID

	// Durable first, acknowledge second. The sender's retry is the only safety net
	// that exists if the queue write did not land.
	if err := r.queue.Enqueue(req.Context(), rec.Tenant+"/"+rec.EventID, raw); err != nil {
		r.finish(w, rec, "not_enqueued", http.StatusServiceUnavailable)
		return
	}
	r.finish(w, rec, "queued", http.StatusAccepted)
}
```

Two details in there earn their keep. The idempotency key is tenant plus message id, so a sender retry collapses into the same queue entry instead of a duplicate grade write. And the audit record carries a digest of the body rather than the body, which means the record can live in a long-retention store that auditors can read, while the payload itself lives in a queue with a retention window measured in days.

**The access review artifact is the audit stream, not the code.** What you hand the person signing it is a per-tenant table: message counts by outcome, the key id in force for each range of dates, the list of principals that could read the queue, and the retention on each hop.

## What to verify before you sign, and how to back out

Capacity first, because the acknowledgement budget is a capacity question wearing a security costume. If your December roster push is 20,000 events inside a ten-minute window, that is a mean of roughly 33 events per second, and senders that fan out from a batch job rarely arrive evenly — plan the receiver for a multiple of the mean, not the mean. An HMAC over a few kilobytes is cheap; the queue write and TLS termination are what you actually size. I set the objective as p99 acknowledgement under 250 ms measured at the edge, and I treat sender-side timeouts as the SLI that matters, since a sender that gives up produces retries that look like duplicate enrollment changes to the consumer.

Then run the negative tests, ideally in CI against the real handler:

```bash
TS=$(date +%s)
ID=msg_7f3c9a2e
BODY='{"event":"roster.updated","tenant":"district-41","student_count":12}'
SIG=$(printf '%s.%s.%s' "$ID" "$TS" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" -binary | base64)

curl -sS -o /dev/null -w '%{http_code} %{time_total}\n' \
  -H "webhook-id: $ID" -H "webhook-timestamp: $TS" -H "webhook-signature: v1,$SIG" \
  -H 'content-type: application/json' --data-raw "$BODY" \
  https://intake.example.edu/hooks/district-41
```

Flip one byte of `$BODY` after signing and expect 401. Set `TS` to two hours ago and expect 401. Send the same id twice and expect one queue entry and two audit records. Send 300 KB and expect 413. Those four cases are the whole security story, and they take about twenty minutes to write.

Rollback deserves a plan that is not "revert the deploy". If verification starts rejecting live traffic — a sender changed its encoding, a key was rotated on one side only — the tempting move is a flag that skips the check, and that flag is exactly what the access review will find and object to. The safer lever is a quarantine path: keep verifying, keep rejecting, and additionally persist unverifiable requests to a separate encrypted store with a 7-day lifecycle and an explicit reader list, so the payloads are recoverable but the failure stays visible. During a key rotation, both keys are active, so the ordinary rollback is to leave the previous key in the keyring for the overlap window and revoke it once the audit stream shows zero requests signed by it.

The build-versus-buy call usually gets made on this axis rather than on cost:

| Option | Who holds the signing key | Evidence you can export | On-call surface |
| --- | --- | --- | --- |
| Managed gateway plus hosted queue | Platform vendor's secret store | Vendor's request logs, subject to their retention and export limits | Small; you own consumers only |
| Self-hosted receiver plus self-hosted broker | Your secret store, your rotation record | Whatever you emit, at whatever retention you choose | You own TLS, brokers, disks, upgrades |
| Function-per-event on a serverless platform | Platform secret store | Platform logs, usually short retention by default | Small, but cold starts complicate the ack objective |

I have watched teams pick the middle row purely for the second column, which is a defensible reason when the auditor's questions are specific and the vendor's export is not.

## When a queue is the wrong answer

If a district sends 200 events a day and the consumer is a single process writing to one idempotent table, the queue is a second system to patch, monitor, and staff, and you should stick with synchronous processing inside a strict handler budget. The audit record still applies. Queues earn their place when the consumer is slower than the sender is patient, or when you need replay.

If the sender supports thin payloads — an event that carries only an id, which you then fetch over an authenticated API — take it, because the custody problem mostly disappears when no student data lands in your queue in the first place. That design costs you an extra round trip per event and a dependency on the sender's availability at read time.

HTTP Message Signatures (RFC 9421) is the better mechanism on paper, with a defined signature base covering headers and a proper key identifier, and it is worth asking vendors about at contract time. Support across senders is uneven, and you don't get to choose what a vendor emits. I'm not sure a per-request audit record satisfies an auditor who wants field-level lineage on grade changes rather than message-level evidence — that question belongs to whoever owns the compliance program, and the answer changes what the consumer must log, not what the receiver must do.

## References

- Standard Webhooks specification — https://www.standardwebhooks.com/
- RFC 9421, HTTP Message Signatures — https://www.rfc-editor.org/rfc/rfc9421.html
- RFC 2104, HMAC: Keyed-Hashing for Message Authentication — https://www.rfc-editor.org/rfc/rfc2104
- OWASP Secrets Management Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- OWASP Logging Cheat Sheet — https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- Express 5 API reference, `express.raw` — https://expressjs.com/en/api.html
- Go standard library, `crypto/hmac` — https://pkg.go.dev/crypto/hmac
- U.S. Department of Education, Student Privacy Policy Office — https://studentprivacy.ed.gov/
