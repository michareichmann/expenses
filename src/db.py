from contextlib import contextmanager
from typing import Type

import pandas as pd
from sqlalchemy import create_engine, select, Select, Table
from sqlalchemy.orm import sessionmaker, scoped_session

from src.tables import Base, MyBase
from src.utils import bytes2str

DATABASE_URL = 'sqlite:///example.db'

# Engine as a module-level singleton
engine = create_engine(DATABASE_URL, echo=False)

# Session factory / scoped session (safer for threaded apps)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False,
                                           autocommit=False))


def init_db() -> None:
    """Create tables (call once at startup)."""
    Base.metadata.create_all(engine)


def read_table(table: Type[MyBase]) -> pd.DataFrame:
    with get_session() as session:
        return pd.read_sql(select(table), session.get_bind())


def read_sql(query: str | Select) -> pd.DataFrame:
    with get_session() as session:
        return pd.read_sql(query, session.get_bind())


@contextmanager
def get_session():
    """Yield a DB session and ensure it is closed."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def list_table_sizes():
    """ Print per-table sizes (bytes) using dbstat if present. """
    with get_session() as s:
        # dbstat returns page-level sizes per object; sum by object name
        t = Table('dbstat', Base.metadata, autoload_with=engine)
        q = select(t).where(~t.c.name.contains('autoindex'),
                            t.c.name != 'sqlite_schema')
        df = pd.read_sql(q, s.get_bind()).groupby('name')[['ncell', 'pgsize']].sum()
        df['pgsize'] = df['pgsize'].apply(bytes2str)
        df = df.rename(columns={'pgsize': 'size'})
        return df.sort_values('size', ascending=False)
