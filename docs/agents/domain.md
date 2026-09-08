# Domain documentation

## Repository context

This repository uses a single shared context. Read root `CONTEXT.md` for current architecture and product context, and consult `docs/adr/` for architectural decisions before making a change that crosses a documented boundary.

## Working rules

1. Use the vocabulary and concepts defined in `CONTEXT.md` and relevant ADRs.
2. When a proposed change conflicts with an ADR, flag the conflict and request or record an explicit architectural decision rather than silently bypassing it.
3. Update the applicable context or ADR documentation when an approved change materially alters the repository's architecture, domain model, or operating assumptions.
4. Keep implementation work scoped to the context that owns it; this repository has no separate monorepo package contexts.
