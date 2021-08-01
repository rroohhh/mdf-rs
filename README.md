# mdf-rs
Rust parser for the `mdf` file format used by Microsoft SQL Server. 

## Features
- low level parsing of pages and records
- parsing of system tables
  - parsing of existing tables
  - parsing of table schema
- reading table rows 
- reading LOB data storage
- larger than memory files
- data recovery from broken files
- supported datatypes
  - `tinyint`, `smallint`, `int`, `bigint`
  - `binary(n)`, `char(n)`, `nchar(n)`
  - `varbinary(n)`, `varchar(n)`, `nvarchar`
  - `bit`
  - `sqlvariant`
  - `sysname`
  - `datetime`, `smalldatetime`
  - `uniqueidentifier`
  - `image`
  - `ntext`
  - `float`

## Usage
This crate provides only parsing functionality, for flexibility all pages have to be provided by implementing the `PageProvider` trait.

You can find a example implementation for reading directly from Microsoft SQL Server backup files in the [mtf](https://github.com/rroohhh/mtf-rs) crate.

Access on the `Page` and `Record` level is available from the `PageProvider::get` and the `RawPage::records` API.

The system tables are parsed by `DB::new` and then used to provide a list of existing `Table`s with `DB::tables`.

The rows of a `Table` can be read using `Table::rows` and basic data recovery can be performed using `Table::scan_db`.

Included are additionally three examples:
1. `lob_dumper` scans the whole database for rows entries stored as LOB data and dumps them into files.
2. `p_min_len_dumper` performs a basic for of data recovery by using the `p_min_len` field of `Page`s to associate each `Page` in the database with its corresponding `Table`.
3. `sharepoint_dump` dumps all files stored in a sharepoint database to disk, extracting the names / paths from the `AllDocs` table and their content from `AllDocStreams`.

## Why not `oxidized-mdf`?
I was very delighted to find a existing implementation of a `mdf` file format parser at https://gitlab.com/schrieveslaach/oxidized-mdf, however in the end
I decided to implement a version myself due to multiple reasons:
1. `oxidized-mdf` contains multiple subtle errors, finding them all might be hard
2. `oxidized-mdf` is unsuitable for larger than memory files as is
3. implementing a async interface as `oxidized-mdf` strives do to is hard while supporting larger than memory files

## References
The documentation of the `mdf` format from the following sources was used:
[1] https://github.com/improvedk/OrcaMDF
[2] https://www.sqlskills.com/blogs/paul/category/inside-the-storage-engine/
[3] http://www.kazamiya.net/en/mssql_4n6-01
[4] https://gitlab.com/schrieveslaach/oxidized-mdf
