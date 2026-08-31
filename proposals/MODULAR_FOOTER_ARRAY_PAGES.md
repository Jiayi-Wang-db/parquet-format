<!--
  - Licensed to the Apache Software Foundation (ASF) under one
  - or more contributor license agreements.  See the NOTICE file
  - distributed with this work for additional information
  - regarding copyright ownership.  The ASF licenses this file
  - to you under the Apache License, Version 2.0 (the
  - "License"); you may not use this file except in compliance
  - with the License.  You may obtain a copy of the License at
  -
  -   http://www.apache.org/licenses/LICENSE-2.0
  -
  - Unless required by applicable law or agreed to in writing,
  - software distributed under the License is distributed on an
  - "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  - KIND, either express or implied.  See the License for the
  - specific language governing permissions and limitations
  - under the License.
  -->

# Modular footer array pages

---
Author: Jiayi Wang
Created: 2026-08-28
Name: Store modular-footer arrays in independently encoded pages
Issue: https://github.com/apache/parquet-format/issues/530
Status: DRAFT
---

## Description

The modular footer remains a collection of typed Thrift modules. Array-valued fields do not store
their values in Thrift lists or `binary` fields. Instead, each field contains an `ArrayPage`
descriptor pointing to a raw encoded payload elsewhere in the file:

```text
ModularFooter
  -> PlacementModule (compact Thrift)
       -> data_page_offsets: ArrayPage
            -> raw encoded offsets
       -> total_compressed_sizes: ArrayPage
            -> raw encoded sizes
  -> RowGroupStatisticsModule (compact Thrift)
       -> column_offsets: ArrayPage
            -> ColumnStatistics for projected column
                 -> min_values: ArrayPage
                      -> raw encoded minima
```

`ArrayPage` is both the location and header of the payload. There is no additional Thrift page
header at the payload offset:

```thrift
struct ArrayPage {
  1: required i64 offset;
  2: required i32 length;
  3: required ArrayEncoding encoding;
  4: required i32 num_values;
  5: required ArrayEncodingParameters parameters;
}
```

The containing typed field supplies the values' meaning, type, and indexing domain. `ArrayPage`
supplies only the physical encoding. The normative definitions are in
[`ModularFooter.thrift`](../src/main/thrift/ModularFooter.thrift).

The initial format defines two uncompressed encodings:

* `BIT_PACKED` for dense positional arrays.
* `SPARSE` for optional arrays with relatively few present positions.

## Rationale

Compact Thrift lists are sequential. Finding element `i` in a `list<i64>` requires parsing all
preceding compact integers. This makes a narrow projection walk metadata for columns it will not
read. Storing custom encodings in Thrift `binary` fields restores random access, but makes the
Thrift schema misleading: the actual type and representation are hidden inside opaque bytes.

An `ArrayPage` makes the boundary explicit:

* Thrift defines the footer schema, module boundaries, field meaning, locations, and encoding tags.
* Raw payloads contain the independently addressable arrays.

Typed module structs are retained because they make the format understandable and evolvable. They
are page directories, not containers for the metadata values themselves. Adding an optional
metadata field is an ordinary optional Thrift-field addition.

## Modules

Modules preserve independent read lifecycles:

| module | representation | read behavior |
| :-- | :-- | :-- |
| Schema | ordinary typed Thrift | Read in full to interpret projected columns. |
| Placement | typed fields containing `ArrayPage` | Required to locate projected column chunks. |
| Row-group statistics | per-column typed descriptors | Read only when pruning uses them. |
| Offset index | per-chunk typed descriptors | Read only for selected column chunks. |
| Column index | per-chunk typed descriptors | Read only for selected column chunks. |
| File metadata | ordinary typed Thrift | Read only when descriptive metadata is needed. |

Placement and statistics never share a module or payload. A reader can locate column data without
fetching, decrypting, or understanding statistics.

The root contains typed locations rather than a generic module registry:

```thrift
struct ModularFooter {
  1: required i32 version;
  2: required i32 num_row_groups;
  3: required i32 num_columns;
  4: required i64 num_rows;
  5: required list<i64> row_group_num_rows;
  6: required ModuleLocation schema;
  7: required ModuleLocation placement;
  8: optional ModuleLocation row_group_statistics;
  9: optional ModuleLocation offset_index;
  10: optional ModuleLocation column_index;
  11: optional ModuleLocation file_metadata;
}
```

Each location identifies one independently compact-Thrift serialized module. The outer file
framing that locates `ModularFooter` is specified separately.

## Logical indexing

Placement arrays use column-major **chunk space**. For leaf column `c` and row group `g`:

```text
chunk_index = c * num_row_groups + g
```

Therefore all row groups for one projected column form one contiguous logical slice. A reader can
address that slice without decoding values for preceding columns.

The row-group-statistics module has a directory containing one independently serialized
`ColumnStatistics` descriptor per leaf column. Arrays in that descriptor use row-group ordinal as
their logical position. Per-column arrays contain `num_columns` positions. Arrays inside an
`OffsetIndexChunk` or `ColumnIndexChunk` use that column chunk's data-page ordinal as their logical
position.

`ArrayPage.num_values` always describes the complete logical domain. Under `SPARSE`, absent
positions are included in `num_values` but have no entry in the values stream.

## Common packed-stream rules

Packed integers use little-endian bit order. Within a byte, the least-significant bit is consumed
first. Values have no padding between them. The final byte is padded with zero bits; readers MUST
reject nonzero padding bits.

For `n` values of width `w`, the stream occupies:

```text
ceil(n * w / 8) bytes
```

The containing Thrift field defines the value type:

* `BOOLEAN` uses width 1.
* `UINT32` permits widths 0 through 32.
* `UINT64` permits widths 0 through 64.
* `BYTE_ARRAY` uses a packed cumulative-offset stream followed by concatenated bytes.

Width zero represents an all-zero integer or offset stream and consumes no payload bytes. All
derived length arithmetic MUST be checked for overflow before reading or allocating memory.

## BIT_PACKED encoding

`BIT_PACKED` stores one value at every logical position.

For integer and boolean fields, the payload is one stream of `num_values` fixed-width values. Value
`i` begins at bit offset `i * bit_width`, so it can be read without decoding preceding values.

For a `BYTE_ARRAY` field, the payload is:

```text
[num_values + 1 packed cumulative offsets][concatenated bytes]
```

The first offset MUST be zero, offsets MUST be nondecreasing, and the last offset MUST equal the
length of the concatenated byte region. Value `i` is
`data[offset[i] .. offset[i + 1]]`.

Writers MUST use the minimum width needed for the largest integer or offset. Readers MUST accept a
larger valid width.

## SPARSE encoding

`SPARSE` distinguishes an absent field from a present zero or empty value. Its payload begins with
`num_present` sorted logical positions:

```text
[packed present positions][packed present values]
```

Positions MUST be strictly increasing and less than `ArrayPage.num_values`. They use
`position_bit_width`. A reader finds logical position `i` by binary-searching this fixed-width
stream. If found at present ordinal `k`, its value is value `k`; otherwise the field is absent.

Integer and boolean values form a fixed-width stream using `value_bit_width`. For a `BYTE_ARRAY`
field, the values region is:

```text
[num_present + 1 packed cumulative offsets][concatenated bytes]
```

This gives `O(log num_present)` position lookup and direct access to the corresponding value,
without a bitmap rank operation or prefix scan.

`SPARSE` MUST NOT be used merely to omit zero values. A missing position means the corresponding
source field was absent. A present zero remains in the values stream.

## Typed placement and statistics

The placement module explicitly names every required array:

```thrift
struct PlacementModule {
  1: required ArrayPage data_page_offsets;
  2: required ArrayPage first_dictionary_pages;
  3: required ArrayPage dictionary_page_offsets;
  4: required ArrayPage total_compressed_sizes;
  5: required ArrayPage total_uncompressed_sizes;
  6: required ArrayPage num_values;
  7: required ArrayPage codecs;
  8: required ArrayPage physical_types;
  9: required ArrayPage is_fully_dictionary_encoded;
}
```

Placement fields are dense and use `BIT_PACKED`. Statistics first select a leaf column:

```thrift
struct RowGroupStatisticsModule {
  /** num_columns + 1 offsets locating ColumnStatistics descriptors. */
  1: required ArrayPage column_offsets;
}

struct ColumnStatistics {
  1: optional ArrayPage null_counts;
  2: optional ArrayPage min_values;
  3: optional ArrayPage max_values;
  4: optional ArrayPage min_is_exact;
  5: optional ArrayPage max_is_exact;
  6: optional ArrayPage nan_counts;
}
```

Equal adjacent column offsets mean that the leaf column has no row-group statistics. Within a
`ColumnStatistics` descriptor, an optional field being absent means that no row group has that
statistic. When only some row groups have it, the field is present and its page uses `SPARSE`.
`min_values` and `max_values` MUST use identical present positions; a writer MUST NOT encode one
bound without the other. Exactness values are defined only for those same positions.

## Page indexes

Expanding every per-page array into the footer root would make the root proportional to
`num_columns * num_row_groups`. Each page-index module therefore contains one `chunk_offsets`
`ArrayPage` with `num_columns * num_row_groups + 1` absolute offsets.

For chunk index `k`, the range

```text
chunk_offsets[k] .. chunk_offsets[k + 1]
```

contains one compact-Thrift `OffsetIndexChunk` or `ColumnIndexChunk` descriptor. Equal offsets mean
that the chunk has no corresponding index. The selected descriptor then points to its raw per-page
arrays. Reading one projected chunk does not materialize descriptors for other chunks.

## Encryption

Placement remains independently readable. Each `ColumnStatistics` descriptor and its array-page
payloads form one segment that can be encrypted with that column's key, amortizing nonce and
authentication-tag overhead across all row groups and statistic fields. The array-page encoding is
applied before encryption; ciphertext is not treated as packed values.

The exact modular-encryption envelope and AAD construction are specified separately. In
particular, AAD must bind the file, module, column, and encrypted segment identity.

## Evolution

The format evolves at two existing boundaries:

* Add an optional field to a typed module using normal Thrift evolution.
* Add a new `ArrayEncoding` value when introducing a new payload layout.

The meaning of an existing encoding value never changes. For example, a future sparse encoding
with a rank index receives a new identifier rather than changing `SPARSE`.

Each page chooses its encoding independently, so a writer may use a new encoding only where it is
beneficial. A reader can determine support from the containing `ArrayPage` before fetching the raw
payload.

An unsupported optional statistics or index field disables that optimization. An unsupported
required placement field makes the modular footer unusable. During migration, a reader can fall
back to the traditional `FileMetaData`; a modular-footer-only file produces an unsupported-format
error.

## Validation

A conforming reader MUST reject a modular footer when:

* A required schema or placement module is missing.
* A required placement field is missing.
* An `ArrayPage` has an encoding/parameter union mismatch.
* A count, width, offset, length, or derived length is invalid or overflows.
* A payload range falls outside the file.
* Sparse positions are not strictly increasing or exceed the logical domain.
* Byte-array offsets decrease or do not terminate at the data length.
* Paired min/max pages use different sparse positions.
* Parallel arrays have inconsistent logical cardinalities.

Readers SHOULD impose implementation limits on array cardinality and payload length before
allocation.

## Evaluation

The proposal should be compared with the traditional footer and the earlier representation that
placed encoded arrays in Thrift `binary` fields. Evaluation should cover real wide files with few
row groups and files with sparse statistics.

The minimum evaluation reports:

* Total footer and always-fetched tail bytes.
* Thrift module and `ArrayPage` descriptor overhead.
* Placement-read time for 1, 5, and all projected columns.
* Sparse-statistics read time for the same projections.
* `BIT_PACKED` versus `SPARSE` size and lookup cost for eligible fields.
* Per-column encrypted-statistics overhead.
* Malformed-input and cross-implementation round trips.

Success means preserving projection-scaled access and approximately the same size as the opaque
`binary` representation while making the footer schema and its evolution visible in typed Thrift.

## Open questions

1. Should `BIT_PACKED` versus `SPARSE` selection be entirely writer-controlled or use a normative
   size threshold?
2. Should `ArrayPage.length` remain `i32`, like Parquet page sizes, or use `i64` for consistency
   with module ranges?
3. Which array and module size limits should be normative?
