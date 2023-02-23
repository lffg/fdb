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

## Design

### Storage

`fdb` splits data across 4 KiB fixed-size pages. The "raw page" (i.e., the 4 KiB
sequence of bytes) concept is only used by the underlying database layer, the
disk manager, which handles the reading and writing tasks.

As of now, there are two main page kinds:

- Heap pages, which store data tuples in a non-organized manner.
- Overflow pages, which store tuple's variable-sized fields.

#### Heap Pages

Heap pages store record tuples, with no particular structure or order, besides
the tuple format. There is no ordering guarantee, although users may issue a
full-order query, which reorders the entire database table.

Each record may not surpass its page size. Hence, as pages are 4 KiB-sized, a
table schema may not allow for tuples whose field sizes account for more than
those 4 KiB.

Overflow pages allow variable-sized field types (e.g., `TEXT`). The field then
references the overflow page ID containing the actual value.

TODO(lffg): Document slotted page format here. And linked-list style navigation.

#### Overflow Pages

Overflow pages, as per described above, may be used to store variable-sized
field values. Since inlining values with variable size directly on the tuple
would decrease the total amount of tuples each heap page may hold,
variable-sized values which occupy more than 32 bytes of space are moved to
overflow pages.

Unlike tuples, which can't be stored on more than one page, variable-sized
values may require more than one page for storage. As such, this DBMS may
support, e.g., `TEXT` fields with more than 4 KiB of size.

Just like heap pages, overflow pages are also organized as slotted pages. Each
field in the overflow page may reference a field in another overflow page, i.e.,
the "continuation" of a field that can't fit on a single page.

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
