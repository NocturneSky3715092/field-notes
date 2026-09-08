# Private SaaS Documents: Browser Upload, Signed Download URLs, Node.js and Postgres

For a business SaaS product storing large training artifacts, the browser carries the bytes while Node.js and Postgres handle authorization, state, and retention. **Short answer: signed upload and download URLs are the data-plane boundary; Postgres is the control plane that makes a private artifact reproducible and auditable.**

This is a throughput decision before it is an API decision. A 4 GB training archive should not occupy a Node.js request worker while a customer's uplink does its work. The API should spend its request budget creating a narrowly scoped grant, recording the intended artifact, and checking the resulting object. The browser then sends the bytes directly to object storage, with progress reported from the upload request; MDN documents upload progress events for `XMLHttpRequest`.

## How should Node.js and Postgres govern signed browser uploads?

Treat each upload as a state machine, not as a successful HTTP response. The application creates a pending artifact row with a generated object key, tenant or owner, original filename, declared media type, byte-size limit, retention class, and timestamps. The filename is display data. The generated key is the identity used for storage.

The upload endpoint authenticates the caller, checks the tenant's quota and retention policy, inserts the pending row, and returns a short-lived signed upload URL for that one key. The browser uses the method and headers covered by the signature. A download endpoint performs a fresh ownership check against a ready row and then returns a separate short-lived signed download URL. A URL is a bearer capability; possession of it is not a substitute for the database authorization check that precedes issuance.

The completion request is deliberately boring. It names the artifact ID, never trusts a client-provided object key, verifies that the expected object exists and satisfies the recorded constraints, and changes `pending` to `ready` in one conditional update. A duplicate completion must be harmless. An upload that never completes remains pending and is later reconciled against the retention policy.

The control plane starts here.

Store the policy version on the artifact row, because changing a tenant's default later should not silently rewrite the meaning of an existing training run. A reconciler can find pending rows older than the upload-duration budget, locate unreferenced objects, and schedule deletion according to the recorded policy. It should emit counts and ages, not silently tidy everything. For a 4 GB archive, that distinction matters: a retry, a partial transfer, and a completed object with a missing callback are three different operational states, even if the browser displays one failed upload.

## Failure states matter more than the happy-path upload

They separate the two traffic patterns. Small control-plane calls have a latency SLO that covers identity, Postgres, and grant creation. The data-plane transfer has its own success and duration measures, because customer network conditions are not an application-server latency budget. That separation is what keeps capacity planning honest: size Node.js for concurrent metadata work and validation, then size storage and egress for the artifact distribution pattern.

The browser still needs a real transfer policy. Configure the storage origin to permit the exact application origin, method, signed headers, and response headers required by the client. Test that policy with a private sample object in the production-like origin. A signed request can be cryptographically valid while the browser refuses to expose the response because the cross-origin policy is wrong.

Use retry rules that respect the transfer protocol. Do not blindly replay a completed upload, and do not mark a row ready merely because the browser lost its connection after sending the last byte. The completion path must be idempotent and must re-check storage state. For large objects, define a multipart or resumable policy only after measuring the client's failure rate and the storage system's cleanup behavior; a more complicated upload protocol creates more abandoned state to operate.

Here is the control-plane transition in Go. The conditional `UPDATE` is the important part: two completion requests cannot turn one pending row into two logical artifacts.

```go
package artifacts

import (
	"context"
	"database/sql"
	"errors"
)

func Confirm(ctx context.Context, db *sql.DB, tenantID, artifactID string) error {
	result, err := db.ExecContext(ctx, `
		UPDATE training_artifacts
		SET state = 'ready', confirmed_at = CURRENT_TIMESTAMP
		WHERE id = $1 AND tenant_id = $2 AND state = 'pending'`,
		artifactID, tenantID)
	if err != nil {
		return err
	}

	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("artifact is not pending for this tenant")
	}
	return nil
}
```

The storage client belongs behind a small interface so a test can exercise expiry, missing-object, and size-mismatch cases without sending real bytes. Keep the secret used to mint grants on the server. The browser receives only the grant and the artifact ID it needs for the completion call.

## Retention is the contract, not a cleanup afterthought

Retention has two clocks: the database row's business lifetime and the object's byte lifetime. Store the policy version on the artifact row, because changing a tenant's default later should not silently rewrite the meaning of an existing training run. A reconciler can find pending rows older than the upload-duration budget, locate unreferenced objects, and schedule deletion according to the recorded policy. It should emit counts and ages, not silently tidy everything.

The useful failure cases are specific:

1. An expired upload grant leaves the row non-readable; the client requests a new grant for the same pending attempt only under an explicit policy.
2. A successful byte transfer with no completion call leaves an orphan candidate for reconciliation.
3. A completion for another tenant changes nothing because tenant identity is part of the conditional update.
4. A size or content validation failure leaves the object quarantined or scheduled for deletion and the row non-ready.
5. A download grant is issued only after the current authorization check, so an old link is short-lived and does not become a permanent document URL.

Measure pending age, completion latency, upload duration by size bucket, transfer failure rate, bytes stored by retention class, and the ratio of rows to objects. Alert on a rising orphan ratio and on the oldest pending age, not just on API error rate. Those signals expose the gap between a healthy control plane and a failing data plane.

## Which storage boundary keeps verification and rollback operable?

The trade-off is operational ownership, not a checklist of fashionable features. A managed object store reduces the disk, replication, and upgrade work the platform team carries, but it does not remove policy design, tenant isolation, lifecycle review, or egress accounting. A self-hosted S3-compatible system can provide control over placement and network paths, but the team must own capacity forecasts, repair procedures, and the pager. A database large-object design keeps records close to bytes, while making database backup, replication, and vacuum behavior part of the file-throughput problem.

| Boundary | Good fit | The catch |
| --- | --- | --- |
| Managed object storage plus Postgres | Large artifacts, direct browser transfer, and a small platform team | Provider policy, egress, and retention semantics still need review |
| Self-hosted object storage plus Postgres | A team with storage operations and a reason to control placement | Disk failure, upgrades, replication, and capacity become your SLO work |
| Database large objects | Smaller files whose transactional coupling matters more than bulk throughput | Database growth and backup windows can dominate operations |

Do not choose direct browser transfer for a workflow that requires every byte to pass through an in-process scanner before storage, unless that scanner is designed as a separate streaming data plane. For immutable regulated archives, add an explicitly tested immutability mechanism; a signed URL alone is not a retention guarantee. Stick with a proxy or a different storage boundary when the product needs synchronous transformation, network-level inspection, or permanent public links.

Your mileage may vary on the pending timeout. Derive it from observed upload duration by artifact size, the recovery objective, and the longest client pause the product is willing to tolerate; do not pick a round number and call it a policy.

Rollback should be a metadata operation. Generate a new key for every training-artifact revision, point the Postgres record at the selected revision, and retain the old object until the policy says it can be removed. If the new release changes the state machine, stop minting new grants, let existing ready artifacts remain readable through the old path, and replay reconciliation after the schema and workers are compatible. That is slower than a flag flip. It is also a rollback you can explain at 03:00.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- https://developer.mozilla.org/en-US/docs/Web/API/XMLHttpRequest_API/Using_XMLHttpRequest
