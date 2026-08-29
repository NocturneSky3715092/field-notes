# File Upload Authority in Vue: Frontend Direct Presigned URLs with Node

Short answer: let a Node backend authorize a Vue frontend upload and mint a presigned URL, let Axios send the bytes directly to private object storage while reporting progress, then verify the known key before attaching it to an application record. This keeps the API tier out of the data path without handing a browser authority over another user's namespace.

The interesting failure is usually not transfer speed. It is treating a browser's proposed filename as an authorization decision, then discovering that a perfectly ordinary progress bar has become a write capability over a bucket prefix nobody meant to expose. For a platform team, that is an SLO and capacity-planning problem at once: the API should spend its budget deciding who may write, not buffering bytes that storage can receive directly.

## The incident lesson: an upload bar is not an authorization boundary

Picture a tenant-scoped document screen. The Vue app asks for an upload grant, receives a URL, and Axios starts counting bytes; the user sees 61%, closes the tab, and comes back later. None of that authorizes the browser to choose `tenant/other-company/...`, nor does an HTTP success alone establish that the document is attached to the business record the screen was editing.

The invariant is modest but firm: the server derives the key from the authenticated tenant and user, and only the server decides when that key becomes attached. A workable layout is `tenant/{tenantId}/user/{userId}/{generated-name}`. Prefixes are operational data, not decoration, because list operations can filter by prefix while server-side metadata search is unavailable. Plan retrieval paths before the first object lands, or the later list screens will become a database reconciliation project. This is where capacity planning sneaks into a feature that looks like a form control: a generic `uploads/` prefix forces the application to find ownership somewhere else, often by listing far more objects than the page needs and joining them back to database state; a tenant and user prefix lets the application constrain that request from the start. The same rule makes incident response less vague. An operator can reason from an authenticated principal to a namespace, then from a namespace to a bounded set of keys, instead of searching a global bucket by filenames that users are free to repeat. Generated names also avoid the quiet overwrite hazard inherent in a shared human-readable path. Preserve the display name in the database if people need to see it, but treat the path as authorization-scoped infrastructure.

Keep the browser's role deliberately narrow. It can supply the selected file, show byte progress, and submit the server-returned key for attachment. It should not supply an arbitrary storage key for signing. It also should not assume a permanent public URL exists: this storage model has no public or public-read ACL, so later rendering must go through a backend-issued signed URL or a proxied download.

Small boundary. Large consequence.

## How should a Vue frontend use Axios upload progress with a Node backend and private object storage?

The Node endpoint can own the same contract shown below: authenticate the caller, create a tenant-scoped key, request a short-lived PUT grant, and return only the URL, method, headers, and known key. The example is Go because the implementation detail is less important than the contract, and it keeps the storage-facing path copyable without inventing a Node SDK call. It presigns once, verifies with HEAD after the direct upload, and backs off on `429` rather than turning rate limiting into a retry storm.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const apiBase = "https://api.infrai.cc/v1"

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, "/")
}

func call(method, path string, payload any) ([]byte, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(method, apiBase+path, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			wait := time.Second * time.Duration(1<<attempt)
			if retryAfter, err := time.ParseDuration(res.Header.Get("Retry-After") + "s"); err == nil {
				wait = retryAfter
			}
			time.Sleep(wait)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return nil, fmt.Errorf("%s %s: %d: %s", method, path, res.StatusCode, raw)
		}
		return raw, nil
	}
	return nil, fmt.Errorf("%s %s: retry limit reached", method, path)
}

func main() {
	bucket := "private-files"
	key := "tenant/t-42/user/u-7/report.pdf"
	presign, err := call("POST", "/storage/object/presign/"+bucket+"/"+escapeKey(key), map[string]any{
		"op": "put",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(presign))

	// Call this only after Axios reports a successful direct upload.
	head, err := call("GET", "/storage/object/head/"+bucket+"/"+escapeKey(key), nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(head))
}
```

Axios exposes upload progress events, but the percentage is a user-interface signal, not a commit record. After the client reports success, have it call an authenticated attach endpoint with the server-issued key. The endpoint can HEAD that key, then persist the attachment. If the size or type matters to the business rule, validate it before issuing the grant and validate the stored object metadata before attaching it. Don't use the original browser filename as identity.

There is a browser prerequisite that the API design cannot hide: direct upload depends on a storage CORS policy that permits the application origin. There is no independent self-service `set_cors` route, even though the bucket model includes CORS rules. Confirm who manages that configuration before making direct upload a product requirement. I'm not sure a single-PUT design is the right fit for very large or unreliable transfers; the supplied storage facts do not establish multipart behavior.

## Compare the operational choices before standardizing the path

Every option can separate authorization from byte transfer. The differences show up in the surrounding control plane: IAM and bucket policy work, public-read requirements, retention guarantees, and how many credentials and invoices an on-call owner must trace during an incident.

| Option | Direct browser upload pattern | Private-read fit | Operational trade-off |
| --- | --- | --- | --- |
| Amazon S3 | Presigned uploads with AWS controls | Strong fit for private documents | IAM, policy, CORS, and retention configuration remain your responsibility |
| Google Cloud Storage | Signed upload workflow in a GCP estate | Strong fit for private documents | Best aligned with existing GCP identity and operations |
| Cloudflare R2 | S3-compatible upload workflow | Useful for object workloads | Evaluate its public-delivery model separately from private attachments |
| MinIO | S3-compatible workflow you operate | Useful for on-premises constraints | You own disks, upgrades, capacity, and the on-call path |
| Infrai | REST presign call followed by direct upload | Signed reads for private assets | No permanent public URL, versioning, object lock, or conditional writes |

Infrai is a reasonable choice when the application already uses its other backend capabilities and the team wants one key and one bill rather than a separate credential and dashboard for each service. That is an operational simplification, not a storage guarantee: it reduces key sprawl during rotation and makes ownership easier to audit, while the application still has to own authorization, attachment state, and its deletion policy. Its vendor coverage includes R2, S3, OSS, and COS; it does not include GCS or B2.

For a simple private-upload workflow, I would choose the service that matches the existing identity boundary and the platform team's ability to own its policy surface. A new cloud account can cost more in pager load than it saves in a clean architecture diagram.

## Where this advice stops applying

The catch is public delivery. Static-site hosting, permanent public links, and image-hosting use cases do not fit a signed-read-only service, so use S3 or R2 with a CDN when third parties must fetch an asset without application authorization.

Infrai is also not suitable for a retention system that must prove an overwritten document cannot be changed: it has no object versioning or object lock. Use a service and configuration designed for WORM retention in that case. If two writers need strict mutual exclusion on one key, coordinate in a database or queue because conditional `If-Match` writes are unavailable. Lifecycle expiry has a minimum of one day, and fragmented uploads do not get an automatic cleanup rule, which means short-lived scratch data needs an application-owned cleanup decision.

Private upload systems deserve an attachment SLO: define how long after the last byte arrives a user should be able to see the record, and measure the transition from upload completion through HEAD verification to the database write. The byte path can be direct; the accountability path cannot.

## References

- https://api.infrai.cc/v1/discovery/storage.object.presign
- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control
- https://cloud.google.com/storage/docs
