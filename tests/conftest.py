import sqlite3
from pathlib import Path

import pytest
from dotenv import load_dotenv

load_dotenv('../.env')


@pytest.fixture(scope='session')
def init_tmp():
    test_path: Path = Path(__file__).parent
    tmp_path: Path = test_path / 'tmp'

    if not tmp_path.exists():
        tmp_path.mkdir()

    # NOTE: Auto create the SQLite file before testing.
    # conn = sqlite3.connect(tmp_path / 'pyiceberg.db')

    yield tmp_path
