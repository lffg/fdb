<!-- cspell:ignore irst -->

# fdb — my <em>f</em>irst database

Tiny database for a school project.

## Roadmap

- [x] Simple file format
- [ ] Basic pager
- [ ] Basic operations over records
  - [ ] Create
  - [ ] Read
  - [ ] Delete
  - [ ] Update
- [ ] Public library interface
- [ ] Handle PGN files
  - [ ] Parser
  - [ ] Script to ingest it into a database
- [ ] Sort records by arbitrary key
  - [ ] Basic free-space reclamation algorithm
- [ ] Public application interface (SQL-like language in a REPL)
  - [ ] SELECT
  - [ ] INSERT
  - [ ] UPDATE
  - [ ] DELETE
  - [ ] Edit script to use the SQL interface
- [ ] Revisit file format to support new features
- [ ] User-defined schemas
  - [ ] Schema format in database file
  - [ ] Internal schema representation
  - [ ] Expose library and application interfaces
- [ ] Indexing
  - [ ] B-Tree
  - [ ] Hash
- [ ] Improve Pager
- [ ] Advanced queries
  - [ ] JOIN queries (incl. foreign key support)
  - [ ] Aggregation queries
- [ ] Concurrency control
- [ ] Virtual machine and bytecode
- [ ] Query optimization

## Dataset for tests

Though this database supports arbitrary user-defined schemas, while being
developed, we used the Lichess database for testing purposes. If you also want
to use such a dataset for testing purposes, you may download it at
https://database.lichess.org. Follow the instructions on how to decompress the
archive.

Hence, while not related to the database implementation, this repository also
provides a script to parse the unpacked [PGN] file and ingest it into the
database.

[pgn]: https://en.wikipedia.org/wiki/Portable_Game_Notation

## References

- [CMU Database Systems Course](https://15445.courses.cs.cmu.edu/fall2022/schedule.html).
- [SQLite Database File Format](https://www.sqlite.org/fileformat.html).
- Database Internals: A Deep Dive Into How Distributed Data Systems Work.
