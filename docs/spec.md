This document describes `fdb`'s file format. s

# Main Concepts and Definitions

The database is stored in a single file, which is divided into pages. Each page
occupies 4 KB (4096 bytes) of size. This value may be parameterized in the
future.

## Page types

- First Page
- Heap Pages

# Structure

- `FirstPage`
  - `MainHeader`
    - (...)
  - `ObjectSchema` first section. Where `ObjectSchema` is defined by:
    - `next_id`, the ID to the next `ObjectSchema` page (see note below).
    - Many `Object`s, where each `Object` is defined by:
      - `type`, the type of the object (e.g. table or index).
      - `page_id`, the ID of the first page which stores data of this object.
      - `name`, the name of the object. For example, the name of an user-defined
        table.
    - > The first entry in the object schema will refer to the `fdb_schema`
      > table, that is be automatically bootstrapped by the database engine.
    - > Since the user may introduce new database objects (i.e., tables,
      > indexes, etc) arbitrarily, the object schema may not fit only in the
      > first page. In such a cases, the `next_id` will point to the next page
      > of the object schema. The object schema file representation is similar
      > to the following user-defined table:
      >
      > ```sql
      > CREATE TABLE fdb_object_schema (
      >     type     byte,
      >     page_id  int,
      >     name     character varying(64),
      >     sql_repr blob,
      > );
      > ```
      >
      > Hence, the database implementation may use the same kind of page used by
      > "regular tables" to store the next pages of the object schema.

Each "data page" is stored as a heap pages. Records are stored sequentially and
a record may not surpass the maximum page size.

Pages may be padded with zeroes towards if the next record doesn't fit into such
a portion. In the future, as an optimization, variable-length fields (such as
strings or blobs) will be stored separately, so that this padding doesn't waste
much space. (see below)

### TODO

Each "data page" (e.g. heap pages used to store tables) are formatted as a
slotted page. E.g.:

![slotted page diagram](./assets/slotted-page.png)

Records (arbitrary strings of bytes) are stored in the cell space and may be
freely reordered without affecting external references to it, since such
references points to elements in the pointer section.

Evidently, a pointer in the pointers section may be _removed_. In such cases,
all external references to it must be deleted or marked as so.
