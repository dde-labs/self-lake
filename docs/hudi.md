# Apache Hudi

Hudi-rs uses Apache Arrow under the hood, which makes it easily compatible with
other Arrow-native libraries and tools. 
Apache Arrow is a columnar in-memory data representation that can be utilized
by any processing engine. Arrow is neither a storage nor an execution engine; 
it serves as a language-agnostic standard for in-memory processing and data 
transport.
This means that when two systems "speak" Arrow, there’s no need for additional 
serialization and deserialization of data during transport, enabling seamless 
interoperability and reducing costs.

Hudi-rs scans the records from a Hudi table sitting in a local storage or any
cloud object store (such as S3) and puts them out into [Arrow RecordBatches](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html#).

!!! note

    A record batch is a collection of equal-length arrays that conform to a specific
    schema. It is a table-like data structure, essentially a sequence of fields,
    where each field is a contiguous Arrow array.
