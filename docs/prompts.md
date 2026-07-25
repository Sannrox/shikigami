# Versioned prompts

System prompts are **versioned assets** under `src/prompts/`. Each asset has a
stable name and a content digest used for outcome attribution.

## Scheme

```text
{id}:{sha256_hex}
```

Example: `harness-v1:a1b2…` (64 hex chars).

- Newlines are normalized to LF before hashing.
- Changing the prompt body changes the digest (and thus the versioned id).
- The default asset is `HARNESS_V1` / `DEFAULT_PROMPT` (`harness-v1.md`).

## Attribution surfaces

| Surface | Field |
| --- | --- |
| Local events | `HarnessEvent::Prompt { prompt_id }` at run start |
| Checkpoint | `prompt_id` (resume rejects mismatch) |
| `RunResult` | `prompt_id` |
| Governed harvest | `prompt_id` on begin/complete attributes |

## Tests

```bash
cargo test prompts::
```

Locks the id scheme and content-sensitivity of digests.
