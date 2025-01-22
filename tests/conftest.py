from pathlib import Path

import pytest
from dotenv import load_dotenv

TEST_PATH: Path = Path(__file__).parent
load_dotenv(TEST_PATH.parent / '.env')


@pytest.fixture(scope='session')
def init_tmp():

    current_path = Path('.')

    tmp_path: Path = current_path / 'tmp'

    if not tmp_path.exists():
        tmp_path.mkdir()

    yield tmp_path
