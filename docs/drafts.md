# TODO

- Save `PAGE_SIZE` and `IDENTIFIER_SIZE` in the header. Also, parameterize them
  for testing purposes.
- Add error context.
  - Around `read_var_size_string` and `write_var_size_string`.
- `pager` cache eviction resistance to duplicate `RwLock`s.
- `pager` tests; mock `DiskManager`.
- Buff Trait. P0.

- Primitive heap insert operation.
  - Assuming that the insert operation would be able to insert many records "at
    once", its implementation should avoid performing one flush per record
    inserted.
- Trait over `Db` common operations, such as `flush` etc. This way, one could
  provide a `BufDb` type (`BufDb: Db`), which would be used by the heap
  primitive insert operation to avoid one flush per inserted-record.

Ideias:

- Abstrair operações de cada tipo de página na implementação da própria página?
