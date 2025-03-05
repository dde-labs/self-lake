# Self: lakeHouse

This project was created for research and develop with lake storage engines like
DeltaLake, IceBerg, etc.

**This project will focus on**:

- Open table file format such as Deltalake, Iceberg, and Hudi (I think I can add
  Hive on this project.)
- Storage of these open table format such as MinIO, and HDFS.

## :round_pushpin: Prerequisite

IceBerg Config;

```dotenv
PYICEBERG_CATALOG__SANDBOX__TYPE=sql
PYICEBERG_CATALOG__SANDBOX__URI=sqlite:///./tmp/pyiceberg.db
PYICEBERG_CATALOG__SANDBOX__WAREHOUSE=file://./tmp
PYICEBERG_CATALOG__SANDBOX__INIT_CATALOG_TABLES=true
PYICEBERG_CATALOG__SANDBOX__POOL_PRE_PING=true
PYICEBERG_CATALOG__SANDBOX__ECHO=false
```

:fast_forward: Read more on the [Official Document](https://py.iceberg.apache.org/)

## :speech_balloon: Contribute

I do not think this project will go around the world because it has specific propose,
and you can create by your coding without this project dependency for long term
solution. So, on this time, you can open [the GitHub issue on this project :raised_hands:](https://github.com/dde-labs/self-lake/issues)
for fix bug or request new feature if you want it.
