---
icon: material/chat-question-outline
---

# Apache Hudi

Hudi stands for — Hadoop Upsert Deletes and Incrementals

Apache Hudi (Hadoop Upserts Deletes and Incrementals) is an open-source data management
framework that is designed to simplify incremental data processing and data pipeline
management for large-scale, high-performance data lakes.
It helps us in managing large volumes of data with high velocity.

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

## References

- https://asrathore08.medium.com/apache-hudi-d259c1f202db
