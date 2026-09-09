# ADR 0011: CLI bounded-content process-host request

- Status: Accepted
- Date: 2026-09-09
- Resolves: [Issue #252](https://github.com/Sannrox/shikigami/issues/252)
- Discussion: [#261](https://github.com/Sannrox/shikigami/discussions/261)
- Depends on: [ADR 0006](0006-bounded-content-parts.md)

## Context

ADR 0006 made library embedding the first content intake. Process hosts must
not coerce image, audio, or document bytes into the frozen text `run` contract.
CLI, MCP, serve, and plane intake were left text-only until a versioned
process request and resolver-ownership rule existed. Content replay remains a
separate contract.

## Decision

1. **CLI first.** Additive `shikigami run-content --request FILE --payloads DIR`
   is the only process-host content intake in this slice. MCP, serve, and plane
   intake stay text-only. Text `run` is unchanged.
2. **Versioned request, payloads aside.** `ContentProcessRequestV1` carries
   schema v1 descriptors and a map of opaque references to relative filenames.
   Bytes never appear in the request JSON or on argv. The payload directory is
   the CLI-owned `FileContentResolver` (`cli-file-v1`).
3. **Same execution path.** The CLI builds `ContentRunRequestV1` and calls
   `Harness::run_content`. Oversized, unresolvable, unknown-field, or denied
   content fails before model or tool execution. Checkpoints stay metadata-only.
4. **JSON result is metadata.** `--json` prints `ContentProcessResultV1` with
   the run projection and returned descriptors. Payloads are omitted.

## Consequences

- Operators can run bounded content without embedding Rust.
- Resume requires the same payload directory and original files.
- Content replay and other process hosts remain explicitly unsupported.

## Rejected alternatives

- `run --content` or encoding media as text.
- Putting filesystem paths or `file:` URLs in descriptor `reference`.
- Adding MCP/serve/plane content intake in the same slice.
