# TODO

- Save `PAGE_SIZE` and `IDENTIFIER_SIZE` in the header. Also, parameterize them
  for testing purposes.
- Add error context.
  - Around `read_var_size_string` and `write_var_size_string`.
- `pager` cache eviction resistance to duplicate `RwLock`s.
- `pager` tests; mock `DiskManager`.
- Buff Trait. P0.

Ideias:

- Abstrair operações de cada tipo de página na implementação da própria página?
