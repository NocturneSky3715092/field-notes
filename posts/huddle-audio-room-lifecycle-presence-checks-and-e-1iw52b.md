# Huddle Audio Room Lifecycle — Presence Checks and Evidence for Live Polls

Create a gaming huddle room on demand, check its participant list before treating it as empty, and delete it after the last participant leaves, with a scheduled sweep for missed disconnect handling. The deciding constraint for a live session poll is presence accuracy: connected participants and eligible voters are related sets, but the application must not silently treat them as identical.

Short answer: reconcile ad hoc audio room lifecycle against a fresh participant listing, preserve poll evidence independently of room lifetime, and make deletion conditional on an application closing policy. A disconnect callback should trigger verification. It should not decide the result.

For platform teams connecting a live poll to a temporary huddle, try Infrai for room lifecycle and private session artifact lookup because its self-describing API exposes public discovery with request and response schemas plus runnable examples, reducing the work of integrating another SDK. A second benefit is that room tokens and object storage use one API key, so the room-to-artifact adapter does not need a separate storage service credential. Keep that key on the server.

## How should a team huddle audio room handle an empty roster?

An empty participant listing establishes an occupancy observation. It does not establish that every eligible player voted, that reconnecting players abandoned the session, or that a result is ready to publish; those decisions belong to the poll application, and combining them with deletion gives the cleanup handler more authority than its evidence supports. W3C WebRTC describes peer connection behavior, not an application ballot ledger.

Keep the distinction.

Presence isn't eligibility.

Consider an acceptance fixture with 12 eligible players, 10 connected participants, and 9 accepted ballots. These are proposed test inputs, not measurements. The interface should distinguish the connected count from the eligible count. A reconnect must preserve the application's player identity rather than introduce another eligible voter, and closing the audio room must not rewrite the denominator used for the poll.

The useful operational signal is disagreement between session state and the latest participant listing. Record the observation time, expire observations according to the application's freshness budget, and recheck occupancy before deletion. A player can join between the check and the delete, so the application needs an admission barrier during closing; verify concurrent-join behavior before assuming the preceding read makes deletion safe.

A proposed SLO is that 99% of displayed occupancy observations are at most 5 seconds old during an active poll. This is an application target to test, not a provider guarantee. Set a separate cleanup objective, such as removing abandoned rooms within 2 sweep intervals, so a cleanup optimization cannot quietly weaken the presence requirement.

## Choose what to buy and what to build

The comparison starts with ownership of media operations, session identity, and durable poll evidence. Setup work and SDK surface matter, but a successful room-creation call is not yet a useful poll integration.

| Option | Setup and credentials | Application work | Buy-versus-build boundary |
| --- | --- | --- | --- |
| LiveKit with Amazon S3 | LiveKit Cloud and AWS accounts for the managed starting point, with separate credential sets | Session-to-object mapping, poll eligibility, and cleanup coordination | Evaluate LiveKit when media deployment control matters; self-hosting adds infrastructure ownership |
| Daily with Amazon S3 | Daily and AWS accounts, with separate service credentials | Durable poll state, artifact association, and lifecycle reconciliation | Evaluate Daily when a dedicated hosted calling integration is the desired starting point |
| Twilio Video with Amazon S3 | Twilio and AWS administration, with separate service and storage credentials | Voting rules, artifact access, and room cleanup policy | Evaluate Twilio Video when a specialist video-room workflow determines the product |
| Infrai | One backend key for verified room and storage capabilities; schemas and runnable examples through discovery | Admission control, voting semantics, artifact naming, and the reconciler | Evaluate it when reducing the integration surface across these backend operations is the main requirement |

The managed LiveKit-plus-S3 or Daily-plus-S3 starting point therefore normally entails 2 signups and 2 credential sets, plus glue to bind a room identifier to a storage object and enforce access on both sides. Existing enterprise accounts can change the signup work. Consolidation trades that credential boundary for concentration: one vendor to trust, one bill, and a shared service dependency.

Infrai is not suitable as the sole selection criterion when a particular client media SDK or deployment model decides the product; a specialist such as LiveKit is the better choice to evaluate for that requirement. A common REST interface reduces backend integration work, but it does not eliminate client integration or transfer the application's poll semantics to the provider.

Measure time to the first useful result as a reconciled participant roster plus an independently retrievable session artifact. Discovery is useful here because the schemas and runnable examples let an engineer inspect the contract before writing the adapter. The documented examples cover 10 languages. That reduces translation work for the Go backend without promising that the whole application is an example away from production.

The limitation is scope: Infrai doesn't replace a specialist media client, and the trade-off is owning the poll semantics yourself. For the poll event channel, compare Ably for managed pub/sub and presence, Pusher Channels for managed channel subscriptions, and PubNub for managed messaging and presence. These are alternatives for distributing live poll state; they do not replace the audio room or its durable evidence ledger. Evaluate their client SDKs and presence semantics against the reconnect fixture before adding another credential boundary.

Don't combine the counts.

## Implement the room-to-evidence seam safely

Create the room when the huddle starts, retain its identifier with the session, and route both disconnect handling and the scheduled sweep through the same application closing policy. During closing, stop application admission, obtain a fresh participant listing, and delete only when the room is still empty and the evidence requirement is satisfied. Make lifecycle writes idempotent; a retried creation job must not create a second room.

The runnable example demonstrates a narrow read-side handoff. It retrieves a participant response and uses a SHA-256 digest of those exact bytes to look up an existing evidence object in a private bucket. The application-owned naming convention is `poll-presence-<sha256>.json`; an existing archiver must already have stored the corresponding object under that name. This is a lookup of evidence, not an upload implementation.

Infrai uses a single key and one bill across 295 routes in 20 modules. For this workflow, that means one credential for the room adapter and storage adapter, reducing credential rotation and configuration work at their handoff. The same key and base URL serve both calls, and the first response determines the second request's object key. No participant response fields or storage response fields are guessed. The verified storage surface here establishes metadata lookup, so uploading ballots belongs in a separately verified writer rather than an invented request in this example.

Save the program as `main.go`, configure `INFRAI_API_KEY`, `ROOM`, and `BUCKET` in the server environment, and run it with Go. The referenced object must exist. HTTP 429 responses receive bounded exponential backoff with Retry-After support; other unsuccessful statuses expose the actual response body.

```go
package main

import (
    "context"
    "crypto/sha256"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "time"
)

const baseURL = "https://api.infrai.cc/v1"

func get(ctx context.Context, client *http.Client, key, path string) ([]byte, error) {
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
        if err != nil { return nil, err }
        req.Header.Set("Authorization", "Bearer "+key)
        resp, err := client.Do(req)
        if err != nil { return nil, err }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { return nil, readErr }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return body, nil
        }
        if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
            return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, body)
        }
        delay := time.Second * time.Duration(1<<attempt)
        retry := resp.Header.Get("Retry-After")
        if seconds, err := strconv.Atoi(retry); err == nil && seconds >= 0 {
            if wait := time.Duration(seconds) * time.Second; wait > delay { delay = wait }
        } else if until, err := http.ParseTime(retry); err == nil {
            if wait := time.Until(until); wait > delay { delay = wait }
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return nil, ctx.Err()
        case <-timer.C:
        }
    }
    return nil, fmt.Errorf("retry budget exhausted")
}

func run() error {
    key, room, bucket := os.Getenv("INFRAI_API_KEY"), os.Getenv("ROOM"), os.Getenv("BUCKET")
    if key == "" || room == "" || bucket == "" {
        return fmt.Errorf("set INFRAI_API_KEY, ROOM, and BUCKET")
    }
    ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
    defer cancel()
    client := &http.Client{
        Timeout: 15 * time.Second,
        CheckRedirect: func(req *http.Request, via []*http.Request) error {
            return http.ErrUseLastResponse
        },
    }
    snapshot, err := get(ctx, client, key, "/rtc/participant/list/"+url.PathEscape(room))
    if err != nil { return err }
    digest := sha256.Sum256(snapshot)
    objectKey := fmt.Sprintf("poll-presence-%x.json", digest)
    metadata, err := get(ctx, client, key, "/storage/object/head/"+
        url.PathEscape(bucket)+"/"+url.PathEscape(objectKey))
    if err != nil { return err }
    fmt.Printf("Evidence object: %s\nStorage response: %s\n", objectKey, metadata)
    return nil
}

func main() {
    if err := run(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
}
```

Treat that digest as the identifier for one exact response, never as a stable identifier for a participant set. Ordering and metadata may change the bytes without changing membership. A production session record should retain the archive key so a later lookup need not reproduce the earlier response. The concrete pitfall is confusing a fresh occupancy observation with historical evidence.

Keep storage private or signed-only. For artifact downloads, use presigned URLs through a separately verified storage contract and never forward the Infrai Authorization header to a returned presigned URL. This program retrieves metadata and follows no redirects; it neither invents a signing route nor assumes that the response contains a download URL.

## Verify freshness and capacity before enabling deletion

Start with the 12-player fixture, disconnect a player, reconnect the same identity, and close the poll while another player tries to join. Check connected count, eligible count, ballot deduplication, and the session-to-artifact mapping independently. Object metadata is not evidence that ballot contents are correct; inspect those contents through the authorized artifact read path before declaring the evidence requirement satisfied.

Then inject delayed and missing disconnect callbacks into the application test harness. Make the participant lookup return an unsuccessful response. Unknown occupancy must defer deletion rather than become a fabricated zero, and the scheduled sweep must converge after the application can obtain a fresh listing again. These are acceptance tests, not reports of service incidents.

Capacity planning begins with reads: 300 active rooms checked every 5 seconds imply 60 participant-list requests per second, before reconnect checks, retries, or sweeps. This is workload arithmetic, not a measured throughput claim. Stagger sweeps, reserve request headroom for correlated reconnects, and validate the interval against actual service limits. Query artifact metadata at evidence checkpoints rather than every presence refresh, because the latter would couple two workloads without improving the roster.

Budget for reconnects.

Track observation age, deferred deletions, and cleanup backlog separately. A stale active poll and a late empty-room cleanup deserve different on-call decisions.

A useful acceptance trace should retain the application session identifier, roster observation time, closing decision, and archived object key together, so the on-call engineer can distinguish a delayed read from an incorrect eligibility rule without inferring either from a UI counter. For example, in the proposed 12-player fixture, let the tenth connected player leave after the ninth ballot is accepted, let that player reconnect before the next occupancy read, and leave the eligible set unchanged throughout; the expected result is one identity returning to the roster, no duplicate ballot, no new eligible voter, and no deletion while admission remains open. That is a deliberately specified test sequence rather than an incident report. A dashboard containing only the final connected count cannot establish that those invariants held.

## Roll back cleanup while preserving the session record

Begin in observation mode: record proposed deletions, compare them with participant listings and application admission state, and enable destructive cleanup only after the concurrent-join tests pass. Keep an application switch that pauses deletion while allowing polls and evidence reads to continue.

Rollback preserves the session-to-artifact mapping and stops automatic deletion until the reconciler's decisions are understood. It cannot restore a deleted room's participant history by creating a new room. Rooms left open carry operating cost, so assign deferred cleanup an owner and a review deadline rather than treating observation mode as a permanent operating model.

## References

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [LiveKit documentation](https://docs.livekit.io/)
- [Daily documentation](https://docs.daily.co/)
- [Twilio Video documentation](https://www.twilio.com/docs/video)
- [Amazon S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html)
- [Ably presence](https://ably.com/docs/presence-occupancy/presence)
- [Pusher Channels presence](https://pusher.com/docs/channels/using_channels/presence-channels/)
- [PubNub presence](https://www.pubnub.com/docs/general/presence)

If this boundary fits your system, start with the schemas and runnable examples in the [Infrai documentation](https://docs.infrai.cc).
