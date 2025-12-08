from contextlib import contextmanager
from typing import Type

import pandas as pd
from sqlalchemy import create_engine, select, Select
from sqlalchemy.orm import sessionmaker, scoped_session

from src.tables import Base, MyBase

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
