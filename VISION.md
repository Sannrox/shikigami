# VISION

## Purpose

`shikigami` is the headless agent harness for governed work: a local-first
runtime that executes runs under sekai-chisei and can be delivered by tenkai
without requiring a desktop UI.

## Problem

Agent execution is trapped inside chat apps and one-off CLIs:

- Governance is optional or bolted on after the fact
- The same loop cannot run unattended on a fleet host
- Delivery of the *harness itself* is ad hoc
- UI products reinvent execution instead of sharing a testable core

## Product promise

An operator, CI job, or UI can start a **run**. Shikigami materializes a
workspace, executes under chisei constraints, and harvests evidence into sekai.
tenkai can install and upgrade the harness like any other product.

## Principles

- Headless by default; UI is a client, not the runtime
- Fail closed when required governance evidence is missing
- Library-first: CLI and future hosts share one core
- Runs are countable and inspectable; the product name is not the instance name
- Local-first state for install/scratch; graph truth stays in sekai

## Non-goals

- Replacing sekai-chisei or tenkai
- Becoming a multi-tenant SaaS control plane in v0
- Shipping a desktop shell as part of this repository

## Success signal

A run can be started from `shikigamictl`, constrained by chisei, recorded in
sekai, and the harness binary itself can be published and converged by tenkai.
