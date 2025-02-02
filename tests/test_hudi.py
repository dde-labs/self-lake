from pathlib import Path
from typing import Iterator

import pytest
import pyarrow as pa
import pyarrow.compute as pc
import polars as pl
from hudi import HudiTableBuilder


@pytest.fixture(scope='module')
def hudi_tmp(init_tmp) -> Iterator[Path]:
    hudi_tmp_path: Path = init_tmp / 'hudi.db'

    yield hudi_tmp_path


def test_load_hudi(hudi_tmp):
    hudi_table = (
        HudiTableBuilder
        .from_base_uri(str((hudi_tmp / "demo-table").resolve()))
        .build()
    )
    print(hudi_table)

    # records_cloud = hudi_table.read_snapshot()
    # arrow_table = pa.Table.from_batches(records_cloud)
    # df = pl.from_arrow(arrow_table)
    # print(df)
