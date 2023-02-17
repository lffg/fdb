<!-- cspell:ignore irst -->

# fdb — my **f**irst database

Tiny database for a school project.

## Dataset for tests

Though this database supports arbitrary user-defined schemas while being
developed (at the time with a hard-coded schema), we used the Lichess database
for testing purposes.

If you also want to use such dataset for testing purposes, you may download the
dataset at https://database.lichess.org. I suggest downloading the oldest
archive since it is the smallest one. Follow the instructions on how to
decompress the archive.

This repository also provides a script to parse the unpacked [PGN] file and
ingest it into the database.

[pgn]: https://en.wikipedia.org/wiki/Portable_Game_Notation

### TODO

- [ ] Simple storage format
  - [ ] Create records
  - [ ] Read records
  - [ ] Delete records
  - [ ] Update records
- [ ] Handle PGN files
  - [ ] Script to ingest (and parse) PGN files
- [ ] Sort records by their key
- [ ] Expose an interface using a SQL-like language
- [ ] User-defined tables and schemas
  - [ ] Schema internal representation
  - [ ] Arbitrary query execution
- [ ] Rethink the original storage format
- [ ] Indexing
  - [ ] B-Tree
  - [ ] Hash
