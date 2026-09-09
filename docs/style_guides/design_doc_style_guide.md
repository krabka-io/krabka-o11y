# Design Document Style Guide

This guide defines the style and content expectations for design documents in Krabka. The [prose style guide](prose_style_guide.md) defines the wording rules that apply to everything you write here.

## Purpose

Design documents capture **architectural decisions and rationale**, the "why" behind the code. They are for engineers who need to understand a subsystem conceptually before they read the implementation details.

A subsystem's design doc lives at `docs/design.md` inside the crate it describes. Link it from the crate README. A design that spans several crates goes under [`docs/`](../) instead, named `<topic>_design.md`.

## What Belongs in Design Docs

- **Design goals and constraints** that shaped the implementation.
- **Key architectural decisions** and the alternatives considered.
- **Conceptual models** — how components interact, how data and control flow, where the trust boundaries are.
- **Trade-offs** — what the design gave up, and for what benefit.
- **Integration points** — how this subsystem relates to others in the system (the block store, object storage, the write-ahead log, the ingest path, the query API).
- **Upstream interpretation** — how a documented upstream requirement, or an observed upstream behaviour, influenced the design.

## What Does NOT Belong in Design Docs

- **API reference material** — struct fields, enum variants, method signatures (these belong in rustdoc).
- **Usage examples** — code snippets that show how to call an API (rustdoc).
- **Exhaustive lists** — every error type, every flag bit, every field (rustdoc).
- **Implementation details** that do not reflect an architectural choice.

## Document Structure

```markdown
# <subsystem> Design

<One-line description of purpose>

## Design Goals

What properties/qualities was this subsystem designed to achieve?
Why does it exist in this shape rather than a simpler or off-the-shelf one?

## Architecture Overview

High-level conceptual model. How do the pieces fit together?
Diagrams welcome if they clarify relationships.

## Key Design Decisions

### <Decision 1 Title>

What was decided, why, and what alternatives were rejected.

### <Decision 2 Title>

...

## Integration

How does this subsystem interact with other Krabka components
(the block store, the distributor, the compactor, the querier)?
What are the contracts / interfaces?

## Upstream Compatibility

Which upstream behaviours does this implement, and for which signal?
Which suite or corpus establishes each one? Any notable interpretation
decisions, or places Krabka deliberately diverges (and why)?

## Testing

Link to the coverage report and any relevant differential-test suites
(don't duplicate their content).
```

## Research and Verification

When you write or update a design document:

- **Consult the upstream documentation and the upstream source** to make sure the terminology is accurate and the compatibility descriptions are correct. Use the upstream names for query-language constructs, wire fields, and HTTP endpoints. The upstream is Prometheus and Grafana Mimir for metrics, Grafana Loki for logs, Grafana Tempo for traces, and Grafana Pyroscope for profiles.
- **Where the upstream behaviour is undocumented or version-dependent, verify it empirically** against the container image the differential suite pins. Do not rely on a blog post or a wiki. See the root [`README.md`](../../README.md) for the six suites and [`CLAUDE.md`](../../CLAUDE.md) for the rule. Document what you observed and the image you observed it against.
- **Ask clarifying questions** if the code does not make the design intent clear. It is better to ask the maintainer than to guess or to document assumptions that may be wrong.

## Writing Style

- **Be concise but not terse** — brevity is good, but not at the expense of readability. Write in complete sentences with natural flow.
- **Explain the "why"** — decisions without rationale are not useful.
- **Use concrete examples** when they clarify a concept, but not as API documentation.
- **Link to upstream documentation** with section anchors for traceability. Link to the upstream source, to a specification such as OTLP or the pprof format, or to an RFC the same way where they are relevant.
- **Bullets are fine when appropriate** — use them for lists of items, options, or requirements. But each bullet should be a complete thought, not a terse fragment. Design rationale and explanations usually read better as paragraphs.
- **One line per paragraph** in the Markdown source. Let the renderer wrap the text. See the [code style guide](code_style_guide.md#markdown-and-prose-for-docs-you-write).

## Assumed Reader Background

- **Familiar with observability concepts** — time series, labels and label matchers, samples and exemplars, histograms, spans and traces, log streams, profile types and flame graphs.
- **Comfortable with distributed-systems fundamentals** — replication, sharding, write-ahead logs, eventual consistency, and the read path over immutable blocks on object storage.
- **May have limited Rust experience** — explain Rust-specific idioms (ownership, traits, async, `Arc` and lock choices) when they are central to a design decision.
- **Likely background in Go, Java, C, or C++** — comparisons to patterns in those languages, or to how Prometheus, Loki, Tempo, or Pyroscope does something, can help clarify.

When a Rust concept is integral to the design, briefly explain what the concept achieves. Do not assume that the reader knows the idiom. One example is "a single writer task owns the log so we never need a lock across an await".

## Questions to Ask When Writing

1. If I deleted this paragraph, would someone misunderstand the design?
2. Does this explain a decision, or does it only describe what the code does?
3. Could a reader find this information in the rustdoc or the source?
4. Would a new team member understand *why* things are this way?
5. Does a differential suite, a conformance corpus, or a test back every compatibility claim?
