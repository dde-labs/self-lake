from pathlib import Path

import pandas as pd
import ibis
from ibis import _
from duckdb import DuckDBPyConnection
from pyarrow import Table as ArrowTable
from pyarrow.lib import Int64Scalar
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table, Snapshot
from pyiceberg.expressions import EqualTo


def test_load_catalog(init_tmp: Path):
    catalog: Catalog = load_catalog("sandbox")

    assert catalog.properties == {
        'type': 'sql',
        'uri': 'sqlite:///./tmp/pyiceberg.db',
        'warehouse': 'file://./tmp',
        'init-catalog-tables': 'true',
        'pool-pre-ping': 'true',
        'echo': 'false',
    }

    catalog.create_namespace_if_not_exists("default")

    assert (init_tmp / 'pyiceberg.db').exists()


def test_connect_db(init_tmp):
    from sqlalchemy import create_engine, text


    engine = create_engine(url='sqlite:///./tmp/pyiceberg.db')
    with engine.connect() as conn:
        with conn.begin():
            rs = conn.execute(text('SELECT SQLITE_VERSION()'))
            print('SQLite version:', rs.fetchone())


def test_create_table():
    catalog: Catalog = load_catalog("sandbox")
    starwars: ArrowTable = ibis.examples.starwars.fetch().to_pyarrow()
    table: Table = catalog.create_table_if_not_exists(
        "default.starwars", starwars.schema
    )

    df: pd.DataFrame = table.scan().to_pandas()

    # NOTE: Data on this iceberg table does not have any records.
    assert df.shape == (0, 14)


def test_load_data_to_table():
    catalog: Catalog = load_catalog("sandbox")
    table: Table = catalog.load_table("default.starwars")

    starwars: ArrowTable = ibis.examples.starwars.fetch().to_pyarrow()
    table.append(starwars)

    df: pd.DataFrame = table.scan().to_pandas()

    assert df.shape == (87, 14)


def test_inspect_partition():
    catalog: Catalog = load_catalog("sandbox")
    table: Table = catalog.load_table("default.starwars")

    rs: Int64Scalar = table.inspect.partitions()["record_count"][0]
    assert rs.as_py() == 87


def test_remove_with_value():
    catalog: Catalog = load_catalog("sandbox")
    table: Table = catalog.load_table("default.starwars")

    table.delete(EqualTo("species", "Droid"))

    assert table.inspect.partitions()["record_count"][0].as_py() == 86


def test_get_snapshot():
    catalog: Catalog = load_catalog("sandbox")
    table: Table = catalog.load_table("default.starwars")

    snapshot: Snapshot | None = None

    for snapshot in table.snapshots():
        print(type(snapshot))
        print(snapshot)

    df: pd.DataFrame = table.scan(
        selected_fields=("name", "species"), snapshot_id=snapshot.snapshot_id
    ).to_pandas()

    print(df)


def test_group_by_via_duckdb():
    catalog: Catalog = load_catalog("sandbox")

    def species_counts(
        cat: Catalog, conn: ibis.BaseBackend
    ) -> pd.DataFrame:
        table: Table = cat.load_table("default.starwars")

        conn.con: DuckDBPyConnection
        table.scan(selected_fields=("species", )).to_duckdb(
            "starwars_species", connection=conn.con
        )
        expr = (
            conn.table("starwars_species")
            .group_by("species")
            .aggregate(species_count=_.species.count())
            .order_by(_.species_count.desc())
            .limit(10)
        )
        return expr.to_pandas()

    con = ibis.duckdb.connect()
    print(type(con))
    print(species_counts(catalog, con))


def test_count_via_duckdb():
    catalog: Catalog = load_catalog("sandbox")

    def homeworlds_counts(
        cat: Catalog, connection: ibis.BaseBackend
    ) -> pd.DataFrame:
        table = cat.load_table("default.starwars")
        table.scan(selected_fields=("homeworld", )).to_duckdb(
            "starwars_homeworlds", connection=connection.con)
        expr = (
            connection.table("starwars_homeworlds")
            .group_by("homeworld")
            .aggregate(homeworld_count=_.homeworld.count())
            .order_by(_.homeworld_count.desc())
            .limit(10)
        )
        return expr.to_pandas()

    con = ibis.duckdb.connect()
    print(homeworlds_counts(catalog, con))