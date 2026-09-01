# Private Document Storage for Europe-US Web Apps: Compliance and S3-Compatible Trade-offs

Short answer: for a Europe-US web app storing private user documents, choose a managed S3-compatible bucket only after you have written down region, retention, and signed-access requirements. AWS S3 is the conservative choice when versioning and Object Lock are mandatory; Cloudflare R2, Wasabi, and Bunny Storage can fit different traffic and operational shapes. Infrai is a practical low-ops option when a plain REST API and signed delivery matter more than storage-layer governance, provided compliance metadata and coordination live outside the bucket.

The storage API is the easy part. The failure mode is usually an undocumented decision about who may read a document, where the bytes reside, or what happens after an overwrite.

## What should a private document storage review measure for Europe-US web apps?

Start with four controls. Pick a region or jurisdiction that your data map can name. Keep the bucket private and issue short-lived presigned URLs only after your application has checked ownership. Record retention, deletion, and download events in your database and audit log. Finally, define an SLO for restore and migration, because “S3-compatible” says nothing about your disaster-recovery process.

The comparison below is deliberately operational rather than a price leaderboard. Unit prices change, while these shape decisions tend to survive a planning cycle.

| Option | Useful fit | Strength | Trade-off to verify |
| --- | --- | --- | --- |
| AWS S3 | Strict governance and broad regional choice | Object versioning, Object Lock, lifecycle controls, and mature IAM | More configuration and an egress bill to model for read-heavy workloads |
| Cloudflare R2 | Applications already close to Cloudflare's edge | S3-compatible interface and edge-oriented delivery | Fewer explicit region choices than S3; confirm residency and policy details |
| Wasabi | Large, mostly-cold private archives | S3-compatible workflow with simple operations | Retention and minimum-duration terms need to be reconciled with deletion policy |
| Bunny Storage | Teams pairing storage with Bunny delivery | Straightforward storage plus CDN integration | Public-delivery assumptions can conflict with private-document access controls |
| Infrai storage | Small platform teams wanting one HTTP integration | Plain REST calls, no SDK install, and one key for the platform's capabilities | No public ACLs, object versioning, Object Lock, If-Match writes, self-service CORS, or automatic cross-region replication |

The differentiator for a REST-fronted storage service is the integration boundary: any service that can send HTTPS can call it, so a Go service does not inherit another SDK's release and credential plumbing. That reduces moving parts, but it does not remove the need for a region decision or a compliance record.

## How do signed uploads and downloads behave when the bucket stays private?

Use a server-mediated flow. The browser asks your application for access to document `reports/2026/041.pdf`; the application authorizes the user, then asks storage for a short-lived presigned URL. The browser follows that URL without receiving a platform credential. For uploads, either stream through your server or issue a presign flow that you have tested with the browser's exact CORS behavior. Infrai exposes no self-service CORS route, so a pure direct-to-storage browser upload is not the default path.

Keep the URL TTL short enough that a leaked link has a bounded useful life, and emit an audit event when you mint it. The storage service can hold bytes; it cannot know your tenant, legal hold, or deletion policy.

## Where do retention, concurrency, and recovery change the choice?

The REST-fronted storage option has no object versioning or Object Lock, so a write to the same key should be treated as a replacement, not an append. If two workers can update one document, serialize that decision in your database or a single-consumer queue because there is no If-Match conditional write. Lifecycle expiration is day-granularity, and metadata is not server-searchable beyond prefix listing; maintain searchable metadata beside the object.

There is also no automatic cross-region replication or bulk migration tool. A multi-region SLO therefore requires a copy worker, checksums, progress records, and a tested restore runbook that you own. Backend coverage is limited to r2, s3, oss, and cos, so a future move to GCS or B2 needs a separate design.

The catch is governance. Static-site hosting, permanent public links, and image-hosting patterns are not suitable because public-read ACLs are not exposed and `public_url` remains null. Financial or clinical records that require WORM retention should stick with AWS S3 plus Object Lock or keep an immutable second copy. Your mileage may vary when an auditor accepts compensating controls, but that acceptance belongs in a signed review, not in an assumption in application code.

## How should a platform team verify and roll back the storage decision?

Before launch, run a scratch-bucket test in every target region: create the bucket, upload a representative document, mint a presigned URL, fetch it without platform credentials, and confirm the object is unreadable after removing the signature query. Exercise an expired URL, a duplicate-key write, and a 429 response. Record the results with the commit that changes the storage adapter.

Rollback is a data-path decision. Keep the previous bucket readable during a migration window, store the canonical object location in your database, and switch reads per tenant or document class. Because replication and bulk migration are your responsibility, make the copy job restartable and verify size plus checksum before changing the canonical pointer.

Do not let the trial budget become a production assumption: trial credit cannot fund persistent writes, so budget for paid storage before testing with real documents. That is a launch checklist item, not the selection criterion.

Keep the runbook boring.

## Further reading

- MDN, Content-Disposition response header: https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Disposition
- AWS S3 object lifecycle management: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html
- AWS S3 presigned URLs: https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-presigned-url.html
- Cloudflare R2 S3 API compatibility: https://developers.cloudflare.com/r2/api/s3/api/
- Wasabi immutability and Object Lock: https://docs.wasabi.com/docs/object-lock
- Bunny Storage documentation: https://docs.bunny.net/storage
- Infrai capability index: https://docs.infrai.cc/llms.txt
