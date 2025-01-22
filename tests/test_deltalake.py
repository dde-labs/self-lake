from pathlib import Path

import pytest
import polars as pl


@pytest.fixture(scope='module')
def delta_tmp(init_tmp):
    delta_tmp_path: Path = init_tmp / 'delta.db'

    yield delta_tmp_path



def test_delta_create(delta_tmp: Path):
    df = pl.DataFrame({"num": [1, 2, 3], "letter": ["a", "b", "c"]})
    df.write_delta(delta_tmp / "demo-table")