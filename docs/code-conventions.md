# Comment Markers

E.g.

```txt
// <marker>: <message>
```

- `TODO` indicates a behavior that needs fixing in the short-term.
- `XX` indicates a behavior that may need fixing in the medium-term.

Comments without marker are mostly used to justify some design decision.

# Naming Patterns

Since `type` is a keyword in Rust, the identifier `ty` should be used instead
when isolated. In other cases, `type` should be used, e.g. `page_type` or
`PageType`.
