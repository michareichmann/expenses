import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Type

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.tables import Base, TMeta, TExclude, TCategory, MyBase, TFileHash


# todo: data tables
# todo: handle duplicates (see crypto)

class Data(pd.DataFrame):

    DIR = Path(__file__).resolve().parent.parent / 'data'
    DB_PATH = DIR / 'data.db'

    ENGINE = create_engine('sqlite:///example.db', echo=False)
    Base.metadata.create_all(ENGINE)

    _Session = sessionmaker(bind=ENGINE)
    SESSION = _Session()

    def __init__(self, data=None, **kwargs):
        if data is None:
            data = self.load()
        super().__init__(data, **kwargs)

    # --------------------------------------------
    # region INIT
    def load(self):
        return self.read(TExclude)

    @staticmethod
    def read(table: Type[MyBase]) -> pd.DataFrame:
        return pd.read_sql(select(table), Data.SESSION.bind)

    @staticmethod
    def write(df: pd.DataFrame, table: Type[MyBase], index=False):
        return df.to_sql(table.__tablename__, Data.SESSION.bind, if_exists='append',
                         index=index)

    @property
    def table_names(self):
        return list(Base.metadata.tables.keys())

    def _write_meta(self):
        """ update the Meta table. only use if the structure of the input data changed. """
        new = ['Date', 'Exe Date', 'Title', 'Vendor', 'Account', 'Amount', 'Balance']
        TMeta.delete(self.SESSION)
        Data.SESSION.bulk_save_objects([TMeta(tag_type=typ) for typ in new])
        Data.SESSION.commit()
        print(f'updated {TMeta.name} with {len(new)} rows')


class _Base(ABC):

    FNAME: Path
    T: Type[MyBase]

    def __init__(self):
        self.update()

    @property
    def table(self):
        return Data.read(self.T)
    t = table

    @property
    def meta(self):
        return Data.read(TMeta)

    @property
    def view(self):
        s = select(self.T.tags, TMeta.tag_type).join(self.T).order_by('tag_type')
        return pd.read_sql(s, Data.ENGINE)
    v = view

    @classmethod
    def has_update(cls):
        return TFileHash.has_update(Data.SESSION, cls.FNAME)

    def update(self, force=False):
        if not force and not self.has_update():
            return 0
        data = self.read_json()
        meta_cols = self.meta.tag_type.values

        missing = [col.title() for col in data if col not in meta_cols]
        assert len(missing) == 0, f'Could not find {missing} in {TMeta.name}'

        self.T.delete(Data.SESSION)  # wipe the whole table
        n = self.write(data)
        print(f'inserted {n} row{"s" if n > 1 else ""} into {self.T.name}')
        return -1

    def read_json(self):
        data = [(k.title(), v) for k, v in json.loads(self.FNAME.read_text()).items()]
        return dict(sorted(data))

    @abstractmethod
    def write(self, data: dict):
        """ Insert the data of the main table into the DB"""
        pass


class Categories(_Base):

    FNAME = Data.DIR / 'categories.json'
    T = TCategory

    @property
    def view(self):
        s = select(self.T.tags, self.T.category, TMeta.tag_type).join(self.T).order_by('tag_type')
        df = pd.read_sql(s, Data.ENGINE)
        return df.sort_values(['category', 'tag_type']).set_index(['category', 'tag_type'])
    v = view

    def write(self, data: dict):
        meta = self.meta.set_index('tag_type')
        df = pd.DataFrame(data).stack().explode().reset_index(level=0)
        df.columns = ['category', 'tags']
        df = df.join(meta).rename(columns={'id': 'meta_id'})
        df = df.sort_values(['meta_id', 'category'])
        return Data.write(df, self.T)


class Exclude(_Base):

    FNAME = Data.DIR / 'exclude.json'
    T = TExclude

    def write(self, data: dict):
        meta = self.meta.set_index('tag_type')
        df = pd.DataFrame({'tags': data.values()}, index=list(data.keys()))
        df = df.explode('tags').sort_index().sort_index(axis=1)
        df = df.join(meta).rename(columns={'id': 'meta_id'})
        df = df.sort_values(['meta_id', 'tags'])
        return Data.write(df, self.T)
