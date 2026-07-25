---
name: sekai-ontology
description: Consult a local Sekai ontology for explicit classes, relations, validation, and provenance.
---

# Sekai ontology

Use `sekai` when a repository contains a portable ontology database and a
structural answer should come from its explicit definitions and provenance.
The command is single-shot and local; it does not require a server or network.

- Run `sekai --db <path> --json explain <name>` for the resolved definition,
  superclass closure, related definitions, and provenance of a class.
- Run `sekai --db <path> --json query <name> --direction <outbound|inbound|both> --depth <0..32>`
  for bounded traversal from a class. Add `--relation <name>` to follow only
  matching relations. Read `data.classes` and `data.relations`; both are
  deduplicated and ordered by name.
- Run `sekai --db <path> --json entity list`, `entity show <name>`, or
  `relation list` for direct deterministic inspection.
- Run `sekai --db <path> --json validate` before relying on an ontology whose
  definitions may have changed.
- Run `sekai --db <path> --json export` to inspect or exchange the complete,
  versioned logical ontology. The envelope's `data` value can be imported into
  a fresh database with `sekai --db <new-path> import <document-path>`.

Treat ontology output as structured repository evidence. Preserve provenance in
answers, and do not infer facts that the ontology does not contain.
