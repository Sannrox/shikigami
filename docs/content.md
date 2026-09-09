# Bounded content runs

`Harness::run_content` carries ordered text, image, audio, and document
descriptors without changing the stable text-only run contract. The canonical
intake is the Rust embedding API. The CLI adds a thin process host
(`shikigami run-content`); MCP, serve, and plane intake remain text-only.
Content replay is still unsupported. `replay-export` reports content runs as
incomplete (`missing: ["content"]`).

## Before you start

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
filename under `--payloads`. The JSON never contains bytes. The payload
directory is the CLI-owned resolver (`cli-file-v1`). `--json` prints
`ContentProcessResultV1` without payloads. See
[ADR 0011](decisions/0011-cli-content-intake.md).

See [ADR 0006](decisions/0006-bounded-content-parts.md) for authority and
compatibility boundaries.
