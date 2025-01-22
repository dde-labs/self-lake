from pyiceberg.catalog import load_catalog, Catalog


def test_load_catalog():
    catalog: Catalog = load_catalog("sandbox")

    assert catalog.properties == {
        'type': 'sql',
        'uri': 'sqlite:///./tmp/pyiceberg.db',
        'warehouse': 'file:////./tmp',
        'init_catalog_tables': 'true',
        'pool_pre_ping': 'true',
        'echo': 'true',
    }


def test_connect_db(init_tmp):

    assert (init_tmp / 'pyiceberg.db').exists()

    from sqlalchemy import create_engine, text

    engine = create_engine(url='sqlite:///./tmp/pyiceberg.db')
    with engine.connect() as conn:
        with conn.begin():
            rs = conn.execute(text('SELECT SQLITE_VERSION()'))
            print('SQLite version:', rs.fetchone())


def test_create_namespace(init_tmp):
    catalog: Catalog = load_catalog("sandbox")
    catalog.create_namespace_if_not_exists("default")

    assert (init_tmp / 'pyiceberg.db').exists()
