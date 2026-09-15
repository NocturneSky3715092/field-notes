# Edtech AI Image Missing After Retry: Duplicate Overwrite and Same-Key Collision

Short answer: treat each receipt image as an immutable attempt, give it a tenant-scoped key before the upload begins, and let a database transaction select the authoritative attempt. A retry must never write a new image to the same key as an earlier attempt. Without object versioning, a same-key overwrite removes the bytes that an audit may still need, so the fix is a state and namespace design, not a more enthusiastic retry loop.

This matters in an edtech receipt archive because the object is evidence, not merely a thumbnail. A learner, school, or finance team may need to inspect the original file months after an AI pipeline has extracted fields from it. Tenant isolation is therefore the primary decision axis: a correct image in the wrong tenant prefix is a security incident, while a missing image in the right prefix is an audit incident. Both deserve an SLO.

## The audit contract comes before the object key

I would start an incident review with a bounded timeline, not with the storage dashboard. Worker A generates a receipt image and uploads `tenants/t17/receipts/r842/attempts/a01/original.png`. The caller times out after the upload, so the queue makes the job visible again. Worker B generates a different result and writes `tenants/t17/receipts/r842/attempts/a02/original.png`; it is selected in the database. If an older implementation instead pointed both workers at `tenants/t17/receipts/r842/original.png`, A could finish last and overwrite B. Every request could report success while the audit record and the bytes disagree.

That is the invariant: the database selection and the object key are separate identities. An attempt key names one immutable set of bytes. A receipt row names the selected attempt. A human-friendly `latest.png` is a projection, not the record of truth.

The second failure is cross-tenant naming. A key derived from a filename such as `receipt.png`, a model name, or a timestamp is not a tenant boundary. The writer must validate the tenant ID from authenticated job context, then construct the key from that trusted ID and a database-issued receipt and attempt ID. Never accept a complete object key from an untrusted upload request.

Small rule. Big consequence.

## How can an edtech receipt archive prevent a duplicate overwrite when an image is missing after a retry?

Allocate the attempt before generating or uploading the image. The allocation records the tenant, receipt, attempt, and intended object key. A retry of the same transport operation can be idempotent for that attempt; a new generation receives a new attempt ID. This distinction prevents a vague word like “retry” from hiding two different outputs.

The commit order should be boring:

1. Authenticate the tenant and allocate a unique attempt in the database.
2. Upload the original bytes to the attempt key, with tenant and receipt identifiers in metadata where the store supports it.
3. Verify the upload response and content digest before marking the attempt complete.
4. In a transaction, select the completed attempt if the receipt has no winner yet.
5. Resolve audit reads through the selected immutable key; only then create a convenience copy such as `latest.png`.

The database transaction decides the winner. The object store persists bytes. Keeping those responsibilities distinct makes a late worker harmless: it may finish, but it cannot replace a committed selection. A convenience copy can lag or be rebuilt; the selected attempt must remain the authoritative reference.

Here is the narrow part I would test in Go. It deliberately contains no storage SDK. The race is in allocation and selection, so putting it behind an SDK call would make the important rule harder to exercise.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type Attempt struct {
	TenantID, ReceiptID, AttemptID, ObjectKey string
	Complete                                  bool
}

type ReceiptStore interface {
	Allocate(context.Context, string, string, string) (Attempt, error)
	Complete(context.Context, string, string) error
	Select(context.Context, string, string) (Attempt, error)
}

type memoryReceipts struct {
	mu       sync.Mutex
	attempts map[string]map[string]Attempt
	winners  map[string]string
}

func (m *memoryReceipts) Allocate(_ context.Context, tenantID, receiptID, attemptID string) (Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.attempts[receiptID] == nil {
		m.attempts[receiptID] = map[string]Attempt{}
	}
	if existing, ok := m.attempts[receiptID][attemptID]; ok {
		return existing, nil
	}
	a := Attempt{
		TenantID: tenantID, ReceiptID: receiptID, AttemptID: attemptID,
		ObjectKey: fmt.Sprintf("tenants/%s/receipts/%s/attempts/%s/original.png", tenantID, receiptID, attemptID),
	}
	m.attempts[receiptID][attemptID] = a
	return a, nil
}

func (m *memoryReceipts) Complete(_ context.Context, receiptID, attemptID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.attempts[receiptID][attemptID]
	if !ok {
		return errors.New("attempt was not allocated")
	}
	a.Complete = true
	m.attempts[receiptID][attemptID] = a
	return nil
}

func (m *memoryReceipts) Select(_ context.Context, receiptID, attemptID string) (Attempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a, ok := m.attempts[receiptID][attemptID]
	if !ok || !a.Complete {
		return Attempt{}, errors.New("attempt is not complete")
	}
	if winner, exists := m.winners[receiptID]; exists && winner != attemptID {
		return Attempt{}, errors.New("another attempt is already selected")
	}
	m.winners[receiptID] = attemptID
	return a, nil
}
```

The production transaction also needs an authorization check that the receipt belongs to the tenant in the request context. Unit tests should race two completed attempts from one receipt, repeat the same attempt ID, and try selecting an attempt under a different tenant. I would treat an HTTP 409 from a second winner attempt as an expected state conflict, then record it with the receipt ID and tenant ID; the caller shouldn't interpret that response as permission to overwrite the selected object. The expected result is stable: one winner, one tenant namespace, and no database pointer to an uncompleted upload.

## What does no versioning change in recovery and capacity planning?

No versioning means an overwrite is not an archaeological record. Once the old bytes at a key are replaced, the object layer cannot answer which value was there before. That is why unique attempt keys are a prevention control, not a substitute for a retention or backup policy. If the business requirement is to preserve every original receipt, define retention outside the retry code and test restoration separately.

The capacity estimate follows the workflow: receipt volume multiplied by average original size, expected attempts per receipt, and the audit retention window. Failed attempts consume space until a deliberate cleanup policy removes them. Cleanup must be tenant-aware, exclude the selected attempt, and emit an audit event; otherwise a storage-saving job can quietly become the next missing-original incident. I am not sure which retention window fits a particular school or finance policy. The answer belongs in the records schedule and legal review, not in an arbitrary storage default.

GDPR Article 17 adds a real counterweight. An archive cannot treat “immutable” as “never deletable” when a valid erasure obligation applies. Store the relationship between a receipt record and its attempt keys so an approved deletion can remove the original and its derived copies across the archive, indexes, and backups according to the organization’s policy. The deletion workflow needs authorization, evidence, and a defined completion state.

## Buy or build around the tenant boundary?

The application invariant does not change with the storage backend. A managed object service can reduce patching and disk operations; a self-hosted system can provide control over placement and operational policy. Neither choice automatically provides tenant isolation, winner selection, or a safe retry state machine. A managed file system is a different abstraction again: it is reasonable when the workload requires shared file semantics, but generated receipt originals are naturally addressed as immutable objects.

| Decision area | Build in the application | Delegate to a storage service | Evidence to require |
|---|---|---|---|
| Tenant boundary | Authenticate and construct the key | Enforce private access and encryption controls | Cross-tenant read tests and access logs |
| Retry selection | Allocate attempts and commit one winner | Store the resulting bytes | A concurrent-worker test |
| Recovery | Define retention, deletion, and restore policy | Provide the documented durability contract | A restore exercise with an SLO |
| Provider change | Keep a narrow object interface | Operate the selected backend | Export and re-import test data |

The catch is operational ownership. A small platform team should not buy an abstraction merely to avoid writing a six-line key function, but it may rationally delegate transport operations when provider-specific integration and on-call work are the larger burden. Conversely, stay close to a native backend when object governance, conditional writes, version history, or migration tooling is a hard requirement and the team already operates it well. Your mileage may vary; the existing runbooks and recovery drills are stronger evidence than a feature checklist.

## Test deletion and restore, not just upload success

Before release, inject a timeout after upload, make the queue redeliver the job, and allow both workers to finish. Verify that the selected database key still identifies the intended attempt, that a different tenant cannot read it, and that the cleanup process cannot delete it. Repeat with a digest mismatch and with a late completion after a winner is committed.

Measure the SLO that users actually experience: a committed receipt resolves to the selected original, remains isolated from other tenants, and is recoverable for the declared audit window. “Upload returned success” is only a transport observation. It is not proof that the archive is correct.

## References

- https://gdpr-info.eu/art-17-gdpr/
- https://aws.amazon.com/efs/
