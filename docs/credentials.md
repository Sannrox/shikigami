# Credential helper patterns

How to supply plane tokens and model API keys **without** putting secrets in
TOML, git, doctor output, or event streams.

## Recommended pattern: environment variables

| Secret | Settings field | Typical env |
| --- | --- | --- |
| Plane bearer token | `governance.token_env` | `SEKAI_TOKEN` |
| HTTP model API key | `model.api_key_env` | `OPENAI_API_KEY` |

Settings store **names of env vars**, never secret values.

```toml
# examples/governed-sekai-chisei.toml
[governance]
adapter = "sekai-chisei"
endpoint = "http://127.0.0.1:50051"
token_env = "SEKAI_TOKEN"
```

```bash
export SEKAI_TOKEN="…"          # shell / CI secret store / agent host
export OPENAI_API_KEY="…"       # only for ungoverned http model
shikigami --config examples/governed-sekai-chisei.toml doctor
```

### Anti-patterns

- Putting tokens or keys in committed TOML
- Passing secrets on the command line (`--token=…` is not supported)
- Logging `doctor --json` to shared systems without treating it as potentially
  sensitive for other fields (secrets are redacted when present in env)
- Writing secrets into workspace files the agent can `read_file`

## Doctor behavior

`doctor` / `doctor --json` reports only:

- which env var **names** are configured
- whether each is currently **set** or **unset**

If a secret value accidentally appears in a diagnostic string (e.g. error text),
the harness redacts values found in the configured env vars before emitting
doctor lines (`[REDACTED]`).

## Events

Harness event sinks must not include bearer tokens or API keys. Tool args and
model content are host/workspace data — do not put secrets there.

## Optional: OS keyring (operators)

For interactive operator machines, load env vars from the OS keychain before
starting shikigami (no first-party keyring feature required):

```bash
# example pattern (macOS)
export SEKAI_TOKEN="$(security find-generic-password -s sekai-token -w)"
```

Hosts such as onmyoji may inject credentials into the process environment
instead. Shikigami only reads env var names declared in settings.

## CI

Use repository/environment secrets mapped to env vars. Never commit `.env`
files with live credentials.
