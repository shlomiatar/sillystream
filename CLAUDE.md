# Sillystream

Sillystream is an extremely simple system for working with streams: an ordered log of records, cursors, sinks, and a state of up to 64 KB per stream. It is a primitive for agent coordination, event buses and log aggregation.

- `docs/SPEC.md` is the source of truth. It is deliberately small and still being written with the user.
- `docs/old/` holds an earlier, much larger design. It is reference material only and nothing in it is binding. Bring something over only when the user decides to, and don't treat its decisions, invariants or roadmap as settled.

## Rules

- **The runtime never looks at record data**, except on system records (`author = 0`). Data is opaque: producers set it, and it can be compressed, encrypted or anything else.
- **Keep it simple.** Don't add a concept, field or mechanism the spec doesn't have. Raise it with the user first.
- Prefer small, explicit code over generic abstraction.
