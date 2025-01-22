from pathlib import Path

import pytest
import polars as pl
from deltalake import DeltaTable


@pytest.fixture(scope='module')
def delta_tmp(init_tmp):
    delta_tmp_path: Path = init_tmp / 'delta.db'

    yield delta_tmp_path


def test_delta_create(delta_tmp: Path):
    df = pl.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    df.write_delta(delta_tmp / "demo-table")


def test_delta_load(delta_tmp: Path):
    dt = DeltaTable(delta_tmp / "demo-table")
    assert dt.version() == 0
    assert len(dt.files()) == 1

    # NOTE: delta.enableChangeDataFeed = true
    table = dt.load_cdf(starting_version=0).read_all()
    pt = pl.from_arrow(table)
    print(pt.group_by("_commit_version").len().sort("len", descending=True))
