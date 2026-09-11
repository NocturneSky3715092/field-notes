# Object Storage App Data Backups: Node.js Cron and Tenant-Safe Restore

Short answer: use a private, tenant-scoped object-storage layout, make the database dump and signed documents part of one versioned backup manifest, and test the presigned restore path before calling the cron job reliable. Node.js is a reasonable scheduler runtime; it is not a recovery strategy.

For an e-commerce platform, the awkward case is a signed document whose deletion deadline is explicit. The backup has to preserve enough application data to find that document, keep tenant A from reaching tenant B's bytes, and prove that a restore did not quietly mix tenants. Those are separate controls. A successful upload is only one signal.

## Restore reliability starts at the tenant boundary

The first reliability question is not which bucket API the worker uses. It is what a recovery operator is allowed to reconstruct. Define a backup version with a tenant identifier, document identifiers, object keys, content digests, creation time, deletion deadline, and policy revision. Store that manifest with the application database state, or in a durable record that the restore process can verify alongside the dump.

Keep the bucket private. A key such as `tenants/t_1842/documents/d_9021/content.pdf` makes the intended boundary reviewable, but a prefix is not authorization. The restore service authenticates the operator, checks the tenant and document status in trusted metadata, constructs the key itself, and only then issues a short-lived presigned URL for that exact object. A URL is a capability; it does not replace an authorization check.

The deletion deadline belongs in the application record, not only in a storage lifecycle rule. A lifecycle rule is useful enforcement, but it is not the complete audit trail for a contractual retention decision. When a document is replaced, create a new version with its own deadline. Do not let a late retry resurrect an old version under a new name.

Three words: private, scoped, verifiable.

Stop there.

That boundary also defines the failure you should rehearse. A restore that produces an intact PDF for the wrong tenant is worse than a visible missing object, because a checksum cannot detect an authorization error. My release criterion treats an unexpected `200` for a cross-tenant read as a blocker; the expected denial is `403` or the equivalent from the authorization layer.

## How should Node.js cron package app data, database dumps, and a zip for restore?

Make the cron job produce an immutable artifact and a manifest, rather than treating a process exit as proof. The artifact may contain a compressed database dump and metadata needed to locate signed documents; the documents themselves can remain separate objects when per-document deletion and access control matter. A zip is a packaging choice, not a tenant-isolation mechanism.

The sequence should be boring and observable: select a tenant-scoped snapshot, write the dump or archive to scratch space, calculate its digest while reading it, upload it under a deterministic backup version, verify the stored bytes, then commit the manifest. If the process stops after the object write but before the manifest, reconciliation must find the unclaimed object and apply a documented disposition. It must not guess. In practice, the dangerous window is longer than that sentence suggests: a worker can finish the database dump, run out of scratch space while building the zip, retry an upload after a timeout, and leave an operator with two plausible objects but only one durable record. The recovery procedure should compare version, tenant, size, and digest, quarantine the ambiguous artifact, and leave the last verified version available; deleting the first object merely because the second attempt finished would turn an ordinary retry into data loss.

The following Go adapter keeps the provider boundary small. It accepts a server-side key, streams the object through a digest, and gives restore code bytes only after verification. A Node.js cron worker can create the manifest and call an equivalent adapter; the isolation rules do not depend on the language.

```go
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
)

type ObjectReader interface {
	Open(ctx context.Context, key string) (io.ReadCloser, error)
}

func restoreDocument(ctx context.Context, store ObjectReader, tenantID, documentID, expectedHex string) error {
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(documentID) == "" {
		return fmt.Errorf("tenant and document are required")
	}

	// Build the key from trusted identifiers; never accept a client filename.
	key := "tenants/" + tenantID + "/documents/" + documentID + "/content.pdf"
	rc, err := store.Open(ctx, key)
	if err != nil {
		return fmt.Errorf("open retained document: %w", err)
	}
	defer rc.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, rc); err != nil {
		return fmt.Errorf("read retained document: %w", err)
	}
	actualHex := fmt.Sprintf("%x", hash.Sum(nil))
	if actualHex != strings.ToLower(expectedHex) {
		return fmt.Errorf("document digest mismatch")
	}
	return nil
}
```

The code intentionally does not accept a bucket, path, or filename from the caller. Validate identifiers before this function, bind the storage identity to the service, and keep presigned URLs out of logs. Test path traversal strings, a mismatched tenant prefix, an expired document, and replay of an old URL. The database dump must receive the same treatment: restore it into an isolated environment, then verify that each manifest reference resolves to the expected tenant before importing application state.

## A migration rehearsal is the honest restore test

Moving a tenant between storage boundaries is the honest restore test. It proves a chain of evidence, not merely that bytes exist. For each backup version, record the tenant, source snapshot, object key, digest, policy revision, upload result, verification result, and restore result. A `committed` state should require both the object verification and the durable manifest. A `deleted` state should require deletion evidence, not just an elapsed timestamp.

The chain needs two clocks. The application marks a document eligible at its contractual deadline; the storage lifecycle configuration provides a backstop for eligible objects. Lifecycle expiration is not an immediate-delete guarantee, and it does not describe replicas, cached links, exports, or database references. Compare the application record with the storage observation during an audit.

I'm not sure any object-storage lifecycle service can provide the full audit trail an e-commerce retention policy needs. Resolve that uncertainty by recording the policy revision, eligibility timestamp, deletion observation, and responsible worker in the application system; the exact legal evidence still belongs to the data-classification policy and counsel.

Capacity planning belongs in the same review. A restore host needs room for the compressed dump, an extracted zip, streamed document verification, retry overlap, and the largest simultaneous tenant restore. The bucket can be healthy while the restore host runs out of disk. Set a worker limit from peak scratch usage, not average file size, and alert on queue age as well as failed jobs.

Define SLOs for the whole path: authorized retrieval before the deadline, deletion processing after eligibility, and recovery of a representative tenant within the recovery-time objective. Observe upload, verification, manifest commit, presigned issuance, restore, and deletion as separate stages. A green cron metric with no successful restore sample is weak evidence.

## On-call ownership follows the failure mode

This is the buy-vs-build decision I would put in the platform review. The important comparison is ownership of failure modes, not a headline feature count.

| Boundary | Fits when | Trade-off to accept |
| --- | --- | --- |
| Managed object storage | The team wants disk durability and capacity operations outside its on-call rotation | The team still owns identity policy, lifecycle semantics, restore tests, and billing controls |
| Self-hosted S3-compatible storage | Data locality or operational control outweighs the work of running storage | The team owns replication, upgrades, capacity forecasts, incident response, and recovery evidence |
| Database-only document retention | Documents are small and the database has a proven encrypted large-object design | Database growth, backup size, tenant export, and deadline deletion become harder to isolate |

The catch is that no boundary removes the acceptance tests. Choose self-hosted storage when locality or an existing storage operations team is mandatory. Stick with a managed boundary when the team cannot credibly staff replication and capacity incidents. Choose neither as a shortcut if tenant authorization is still implicit in a caller-provided path.

Before deployment, create two test tenants with different documents and deadlines. Run the cron packaging job, verify the dump and zip digest, perform a presigned restore for each tenant, attempt the cross-tenant read, and confirm that an eligible document reaches the intended deletion state. Repeat after changing the retention policy. During a migration, copy one tenant at a time, compare the manifest and digest in the destination, and keep the source immutable until the destination restore has passed. That is the runbook's rollback signal: stop promotion when the manifest, digest, tenant check, or deadline evidence disagrees, preserve the failed backup version for investigation, and restore from the last verified version.

## References

- https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- https://firebase.google.com/docs/storage
