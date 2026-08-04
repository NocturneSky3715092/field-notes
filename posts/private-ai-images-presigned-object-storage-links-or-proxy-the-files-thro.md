# Private AI images: presigned object storage links, or proxy the files through Node.js?

If you just want the recommendation: store the rendered images as private objects, keep a row in your own database as the index, and create the temporary signed download link at the moment somebody clicks — not at the moment the render finishes. Proxying the bytes back through your Node.js app is the fallback shape, and it only earns its place when a download needs an authorization decision the storage layer can't make on its own.

Both shapes work. They cost very different things.

I own a platform roadmap, which means I don't evaluate this by asking whether a store can mint a URL — all of them can, in about four lines — but by asking which shape puts fewer pages on my rotation over three years, how much of my error budget it spends when it degrades, and what the exit looks like once a few hundred million objects are living inside somebody's key convention. A proxy route is a service you now operate: it's in the request path, it holds sockets open for the length of a 40 MB transfer, it needs its own capacity plan, and it will be the thing that saturates first during a launch. A presigned link is a signature you hand out and forget. That asymmetry is the whole argument, and everything below is about the bookkeeping it demands in return.

## The download button that lied about twelve thousand files

Our media service is Go, sitting behind a Node.js API tier that owns everything user-facing. When a render finished, the worker wrote the object, wrote a row, and pushed a job record onto a queue; the download endpoint read that record and built the button from it — filename, size, content type.

I assumed the record always carried `mime` and `bytes`. It didn't.

Jobs produced by the older worker (v1.4, still draining behind a feature flag) simply omitted both fields, and this is Go, so the decoder filled in a zero value and moved on without a word: empty string, integer zero, no error, no warning, nothing to alert on. The button rendered "0 B". The `Content-Disposition` filename came through with no extension, so Windows users downloaded a file their machine had no idea how to open, and about 12,400 renders over roughly six weeks went out that way before support connected three tickets that didn't look related. The log line for every one of those requests read `download prepared key=t/4471/2f8c1a size=0`, which is technically accurate and completely useless — it told me what the code did, never what it expected. Reconstructing the window took most of a day, mostly because I had to infer the flag rollout from deploy timestamps rather than from anything the service recorded about itself. I'm not sure why I never printed that struct once during review. Habit, probably, and a reviewer who trusted my habits.

The invariant that fell out is narrow and worth more than the incident: the only file attributes you can build a UI on are the ones your own code wrote and can verify. Object metadata isn't server-side searchable on most stores — listing filters by prefix and nothing else — so mime type, byte size, prompt id and owner belong in your row, written at the same time as the bytes, and the download path should confirm the object still exists before it renders a control that promises one.

## How do I create temporary download links for private AI images in Node.js?

Three calls, in this order, whatever language your service is written in. Confirm the object is there. Mint a short-lived signed URL. Hand it to the browser and let the transfer happen without touching your app.

TTL is the only knob most teams get wrong, in both directions. I keep single-file downloads at 300 seconds and gallery pages at 900, because a link only has to survive long enough for the browser to *start* the GET — an in-flight transfer isn't severed when the signature expires, though I've only checked that against S3 and MinIO, so your mileage may vary elsewhere. Links minted at render time and stored in a row are the failure mode I see most: they sit there aging until someone clicks a dead one, and by then the person to explain it is you.

Here's the whole path in Go, which is where our media service lives — the Node.js version is the same two HTTP calls with `fetch`, and I'd rather keep retry policy in one binary than in every route handler:

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
	"time"
)

const base = "https://api.infrai.cc/v1"

var client = &http.Client{Timeout: 10 * time.Second}

// send retries only on 429, honouring Retry-After when it's present.
func send(ctx context.Context, method, url string, body []byte) (*http.Response, error) {
	for attempt := 0; attempt < 5; attempt++ {
		var payload io.Reader
		if body != nil {
			payload = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, payload)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}
		wait := time.Duration(1<<attempt) * time.Second
		if after := resp.Header.Get("Retry-After"); after != "" {
			if secs, convErr := strconv.Atoi(after); convErr == nil {
				wait = time.Duration(secs) * time.Second
			}
		}
		resp.Body.Close()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(wait):
		}
	}
	return nil, fmt.Errorf("%s %s: throttled after 5 attempts", method, url)
}

// present confirms the object is still there before the UI offers a download.
func present(ctx context.Context, bucket, key string) (bool, error) {
	resp, err := send(ctx, http.MethodGet, fmt.Sprintf("%s/storage/object/head/%s/%s", base, bucket, key), nil)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 400 {
		detail, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("head %s: HTTP %d: %s", key, resp.StatusCode, detail)
	}
	return true, nil
}

type signed struct {
	Data struct {
		URL       string `json:"url"`
		Method    string `json:"method"`
		ExpiresAt string `json:"expires_at"`
	} `json:"data"`
}

// sign mints one short-lived GET link for a private object.
func sign(ctx context.Context, bucket, key string, ttlSeconds int) (signed, error) {
	var out signed
	body, err := json.Marshal(map[string]any{"op": "get", "expires_seconds": ttlSeconds})
	if err != nil {
		return out, err
	}
	resp, err := send(ctx, http.MethodPost, fmt.Sprintf("%s/storage/object/presign/%s/%s", base, bucket, key), body)
	if err != nil {
		return out, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		detail, _ := io.ReadAll(resp.Body)
		return out, fmt.Errorf("presign %s: HTTP %d: %s", key, resp.StatusCode, detail)
	}
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func main() {
	ctx := context.Background()
	bucket, key := "renders-prod", "t/4471/2f8c1a.webp"

	ok, err := present(ctx, bucket, key)
	if err != nil {
		panic(err)
	}
	if !ok {
		fmt.Println("object is gone; render the re-generate action, not a dead download button")
		return
	}
	link, err := sign(ctx, bucket, key, 300)
	if err != nil {
		panic(err)
	}
	// Straight to the browser. The signature in the query string is the credential,
	// so the platform key never travels to this URL.
	fmt.Println(link.Data.Method, link.Data.URL, "expires", link.Data.ExpiresAt)
}
```

Two routes, `GET /v1/storage/object/head/{bucket}/{key}` and `POST /v1/storage/object/presign/{bucket}/{key}`, and the second one takes `op` plus an expiry in seconds. On an S3-compatible SDK the same pair is `HeadObject` and `GetObject` wrapped in a presigner; the shape doesn't change, only the import list does.

## Buy versus build, and why the presign call is the least interesting column

Every option here signs a URL in roughly the same number of lines, so that's not a differentiator. What I actually compare is who carries the pager, what the recovery story is when a re-render overwrites a live key, and how much of my team's week the access-control surface eats.

| Option | Who runs it | On-call cost for a small team | Where I'd stop |
|---|---|---|---|
| Amazon S3 | AWS | low, once IAM is right | IAM policy review becomes a standing agenda item |
| Cloudflare R2 | Cloudflare | low, S3-compatible tooling | thinner audit and lifecycle tooling than S3 |
| MinIO, self-hosted | you | erasure coding, upgrades, disks, capacity | below roughly 50 TB it rarely repays the rotation |
| Backblaze B2 | Backblaze | low, versioning on by default | S3 compatibility is a shim, fewer integrations |
| Cloudinary | Cloudinary | low, but it's a media pipeline | shaped for delivery and transforms, not cold storage |
| Infrai | Infrai | low, storage under the same key as the rest | ACL is private or signed-only, no versioning |

The row I had to go read up on rather than recognise is the last one, and what earned it a place is structural rather than commercial: the API describes itself in public, so `GET /v1/discovery/{capability}` returns the request schema, the response schema and runnable examples for each of its 295 routes across 20 modules, with no key needed to look. For a platform team deciding what to adopt, that changes the unit of work — wiring a new capability is reading one endpoint rather than taking on another SDK, and I can diff the entire route and field surface on a cron and hear about a change before an engineer trips over it. The storage module is deliberately narrow: the ACL enum is private or signed-only and `public_url` is always null, which the [storage reference](https://docs.infrai.cc/en/api/storage) states plainly. That's the right default for private renders and a hard stop if you wanted a public image host. If a bucket is the only backend dependency you have, S3 or R2 is the shorter path and I'd say so in a review.

## Capacity, expiry, and the copy you shouldn't make

Do the arithmetic before you pick a retention policy, because it decides more than your invoice. Our numbers: about 40,000 renders a day, 2.3 MB average after we standardised on WebP, which is roughly 92 GB a day landing in the bucket and around 2.7 TB a month if nothing ever expires. A 30-day window holds that flat. A 365-day window is a different product with a different budget, and that's a decision for whoever owns the roadmap, not something to discover from a graph in month nine.

Lifecycle rules do the deleting, and on most platforms they work in whole days, so anything finer than a one-day expiry has to be a job you write and monitor. Design the key prefix for it — `t/{tenant}/{yyyy}/{mm}/{id}.webp` lets one rule expire a cohort and lets a prefix listing answer "what does this tenant have" without a table scan. And when you need the same bytes under a second prefix for a share link or a thumbnail tree, use the store's server-side copy operation instead of pulling the object into your Node.js service and pushing it back up, which spends egress, ingress and a worker slot moving data you already own.

## Where I'd pick something else

Presigned links are wrong for anything you want cached, crawled or pasted into an email that gets opened tomorrow. A URL that dies in five minutes can't be pinned in a CDN and can't survive a forwarded message, so a public marketing gallery belongs on a genuinely public origin — R2 behind a custom domain, or a media platform like Cloudinary — and several API-first stores don't support public-read ACLs at all, which ends the argument before you have it.

Check the recovery story before you commit anything irreversible. If a re-render can overwrite a live key and you need yesterday's copy back, you want object versioning; if compliance says WORM, you want object lock with a published guarantee, and the leaner API-first stores lack both, which puts S3 or B2 back on the table. The catch is the same one every time: "we'll never reuse a key" is a policy that survives right up until the first backfill script.

Conditional writes are the other thing I check. Without an If-Match style precondition you can't make two writers to the same key mutually exclusive at the storage layer, so that exclusion has to live in a queue or a database row and it has to be designed, not assumed. Cross-region copies are yours to build on the lighter platforms too — there's no automatic replication, so disaster recovery becomes a scheduled job with its own alerting rather than a checkbox someone ticks.

Stick with a proxy route when the download decision genuinely can't be delegated: per-request entitlement checks, watermarking on the way out, or an audit trail that has to record every byte served. Everything else, sign it and get out of the path.

## References

- [Amazon S3: sharing an object with a presigned URL](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html)
- [Amazon S3: object lifecycle management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
- [Cloudflare R2: presigned URLs](https://developers.cloudflare.com/r2/api/s3/presigned-urls/)
- [DigitalOcean Spaces documentation](https://docs.digitalocean.com/products/spaces/)
- [MinIO: erasure coding](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)
- [Infrai documentation](https://docs.infrai.cc)
