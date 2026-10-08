# Bounded content runs

`Harness::run_content` carries ordered text, image, audio, and document
descriptors without changing the stable text-only run contract. The canonical
intake is the Rust embedding API. The CLI adds a thin process host
(`shikigami run-content`); MCP, serve, and plane intake remain text-only.
Content replay is still unsupported. `replay-export` reports content runs as
incomplete (`missing: ["content"]`).

## Upgrading from 1.x

Version 2.0 changes the public `ResolvedContent::Bytes` enum payload from
`Vec<u8>` to `content::Bytes`, the re-exported immutable `bytes::Bytes` buffer.
Downstream construction and typed match bindings must migrate:

```rust
use shikigami::content::{Bytes, ResolvedContent};

let payload: Vec<u8> = vec![1, 2, 3];
let resolved = ResolvedContent::Bytes(payload.into());
if let ResolvedContent::Bytes(buffer) = resolved {
    let buffer: Bytes = buffer;
    let borrowed: &[u8] = buffer.as_ref();
    let owned: Vec<u8> = borrowed.to_vec();
    assert_eq!(owned, vec![1, 2, 3]);
}
```

Pass a borrowed slice when ownership is unnecessary; `.to_vec()` copies.
`Bytes::clone()` shares the allocation. The descriptor, settings, and content
wire schemas remain v1; this migration affects the Rust library source API.

## Before you start

Binary resolver payloads use `ResolvedContent::Bytes(content::Bytes)`, an
immutable shared buffer. Convert an owned `Vec<u8>` with `.into()`; cloned
buffers share their allocation. Text payloads remain owned `String` values.

A host must own a payload store and implement `ContentResolver`. Shikigami
persists only descriptors and a stable resolver id. The resolver must:

- return the exact bytes bound by each descriptor's length and SHA-256 digest;
- externalize model text, tool arguments, and tool results during the run;
- keep opaque references credential-free;
- preserve payloads for as long as checkpoint resume is required.

The hard v1 bounds are 32 parts, 8 MiB per part, and 16 MiB in aggregate.
`ContentRunRequestV1::capabilities` may lower these values but cannot raise
them.

## Start a run

Create descriptors after putting payloads in the host-owned resolver:

```rust
use std::sync::Arc;
use shikigami::{
    ContentMessageV1, ContentPartDescriptor, ContentRunRequestV1, Harness,
};

async fn run_content(
    harness: &Harness,
    descriptors: Vec<ContentPartDescriptor>,
    resolver: Arc<dyn shikigami::ContentResolver>,
) -> Result<(), shikigami::HarnessError> {
    let messages = vec![ContentMessageV1 {
        role: "user".into(),
        parts: descriptors,
        tool_call_id: String::new(),
        tool_calls: Vec::new(),
    }];
    let mut request =
        ContentRunRequestV1::new("inspect the supplied content", messages, resolver);
    request.keep_workspace = true;

    let result = harness.run_content(request).await?;
    println!("run={} success={}", result.run.run_id, result.run.success);
    Ok(())
}
```

Every descriptor includes a stable part id, kind, normalized media type, byte
length, lowercase `sha256:` digest, opaque reference, provenance, and explicit
disclosure state. Malformed references, unknown media, duplicate ids, size
overflow, resolver drift, or unsupported adapters fail before a model call.
Opaque references must be credential-free 8–256 character identifiers.

Ungoverned scripted runs provide deterministic fixtures for every part kind.
Other model adapters deny content unless they implement the content-specific
port method. Governed runs use only the sekai-chisei content RPC; they never
fall back to the local text or HTTP path. The plane may redact, omit, or reject
parts based on policy and provider capability. Content v1 rejects the
`escalate`/park tool because its request contract has no operator-answer field.

## Resume and export

Resume through `Harness::run_content` with the same initial messages,
capabilities, resolver id, and `resume_run_id`. Shikigami re-resolves every
accepted descriptor and verifies its digest before continuing. A restored
scripted or governed turn does not repeat an already staged model result or
tool effect.

Content checkpoints use two bounded metadata-only sidecar slots under the run
state directory. The ordinary checkpoint binds one exact sidecar generation
and digest, making the ordinary checkpoint the commit point across crashes.
Resolved text and bytes never enter either checkpoint. Content-run tool
arguments are also externalized before checkpointing and restored transiently
through the same resolver if a staged turn resumes.

Use `export_content_transcript` for a metadata-only JSONL projection. It hashes
opaque references and excludes payloads. Ordinary transcript export, ordinary
resume, and replay v1 reject content checkpoints explicitly.

## Security and lifecycle

- Treat descriptor sidecars as sensitive local run state because they contain
  opaque resolver references.
- Do not put URLs, credentials, bearer tokens, query strings, or filesystem
  paths in references.
- Keep resolver output available until the run no longer needs resume.
- Remove payloads through the host store's retention policy; Shikigami does not
  own or silently delete them.
- Treat configured pre-tool hooks as trusted authorization code: they receive
  the exact transient tool arguments that the tool will execute.
- Expect unsupported kinds or media types to fail closed. A descriptor contract
  does not imply that every configured provider supports every modality.

## CLI

```
shikigami run-content --request FILE --payloads DIR [--json]
```

`--request` is schema-v1 `ContentProcessRequestV1` JSON: task, descriptor
messages, and a `payloads` map from opaque `reference` to a single relative
filename under `--payloads`. Unmapped references fail closed even when a
same-named file exists in the directory. Payload opens do not follow
symbolic links. The JSON never contains bytes. The
payload directory is the CLI-owned resolver (`cli-file-v1`). `--json` prints
`ContentProcessResultV1` without payloads, then exits `0` only when the run
succeeded. Failed or parked runs print the report and exit `1`, matching
`run`. See [ADR 0011](decisions/0011-cli-content-intake.md).

See [ADR 0006](decisions/0006-bounded-content-parts.md) for authority and
compatibility boundaries.

## HTTP attachment encoding cache

The HTTP adapter caches image and PDF data URLs by the digest of the actual
resolved bytes and their MIME type. The cache is shared within the process,
including across ACP adapter reconstruction. Each call still resolves and
validates accepted payloads; redacted and omitted parts are not sent. Changing
the bytes or MIME type produces a different encoding. JSON request construction
still copies the cached string into the wire request.

The cache retains at most 32 entries and 22,373,720 bytes of encoded URLs
(one 16 MiB content history expanded as base64, plus bounded headers). Least
recently used entries are evicted. Concurrent sessions share this budget, so
an evicted attachment may be encoded again. Encodings are never persisted or
logged, but may remain in process memory after a session ends until eviction
or process exit. The host-owned decoded payload store retains its own policy.
