import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Type, Iterable

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.tables import Base, TMeta, TExclude, TCategory, MyBase, TFileHash, TData


class Data(pd.DataFrame):

    DIR = Path(__file__).resolve().parent.parent / 'data'
    DB_PATH = DIR / 'data.db'

    ENGINE = create_engine('sqlite:///example.db', echo=False)
    Base.metadata.create_all(ENGINE)

    _Session = sessionmaker(bind=ENGINE)
    SESSION = _Session()

    category, exclude = [None] * 2

    def __init__(self, data=None, force_update=False, **kwargs):
        if data is None:
            data = self.load()
        super().__init__(data, **kwargs)
        self.update_db(force_update)

        self.category = Categories()
        self.exclude = Exclude()

    # --------------------------------------------
    # region INIT
    def load(self):
        return self.read(TData).set_index('id')

    def read_csv(self, fname: Path):
        cols = [col for col in self.orm_cols if col.lower() != 'id']
        date_cols = [col for col in cols if 'date' in col]
        return pd.read_csv(fname, names=cols, skiprows=1, usecols=range(7),
                           parse_dates=date_cols, dayfirst=True, decimal=',')

    def files_to_update(self, all_=False):
        return [f for f in self.fnames if all_ or TFileHash.has_update(self.SESSION, f)]

    def update_db(self, force=False):
        fnames = self.files_to_update(force)
        if len(fnames) == 0:
            return -1
        df_in = pd.concat([self.read_csv(f) for f in fnames]).drop_duplicates()
        # keep only the rows which are not duplicated (not in the db)
        df_new = pd.concat([self.drop(columns='id'), df_in]).drop_duplicates(keep=False)
        self.write(df_new)
        n0, n1 = len(self), len(df_new)
        self[:] = self.load()
        print(f'inserted {n1} rows into {TData.name} ({n0} -> {n0 + n1})')
        return 0
    # endregion
    # --------------------------------------------

    # --------------------------------------------
    # region UTILS
    @staticmethod
    def read(table: Type[MyBase]) -> pd.DataFrame:
        return pd.read_sql(select(table), Data.SESSION.bind)

    @staticmethod
    def write(df: pd.DataFrame, table: Type[MyBase] = TData, index=False):
        return df.to_sql(table.__tablename__, Data.SESSION.bind, if_exists='append',
                         index=index)

    @property
    def table_names(self):
        return list(Base.metadata.tables.keys())

    @property
    def fnames(self):
        return list(self.DIR.glob('hist*.csv'))

    @property
    def orm_cols(self):
        return [c.name for c in TData.__table__.columns]
    # endregion
    # --------------------------------------------

    # --------------------------------------------
    # region GETTERS
    @property
    def min_date(self):
        return self.date.min()

    @property
    def max_date(self):
        return self.date.max()
    # endregion
    # --------------------------------------------

    def _write_meta(self):
        """ update the Meta table.
        only use if the structure of the input data changed. """
        new = self.orm_cols
        TMeta.delete(self.SESSION)
        Data.SESSION.bulk_save_objects([TMeta(tag_type=typ) for typ in new])
        Data.SESSION.commit()
        print(f'updated {TMeta.name} with {len(new)} rows')

    def contains(self, col: str, lst: Iterable):
        or_str = '|'.join(x.lower() for x in lst)  # noqa Incorrect Type
        return self[col].str.lower().str.contains(or_str).astype(bool).fillna(False)

    def filter_excluded(self):
        df = self.exclude.v.pivot(columns='tag_type')
        df.columns = df.columns.droplevel(0)
        masks = [~self.contains(col, df[col].dropna()) for col in df.columns]
        mask = pd.concat(masks, axis=1).all(axis=1)
        return self[mask]

    def init_cat_table(self) -> pd.DataFrame:
        """ init df for the whole period"""
        dmin, dmax = self.min_date, self.max_date
        i = [(y, m) for y in range(dmin.year, dmax.year + 1) for m in range(1, 13)]
        i = i[(dmin.month - 1):-(12 - dmax.month)]
        ind = pd.MultiIndex.from_tuples(i + [('Total', '')], names=['year', 'month'])
        categories = self.category.v.droplevel(1).tags.items()
        return pd.DataFrame(index=ind, columns=pd.MultiIndex.from_tuples(categories))

    def categorise(self):
        df_data = self.filter_excluded().copy()
        df_cat = self.init_cat_table()
        for (cat, col), tags in self.category.agg_lists().items():
            for tag in tags:
                mask = df_data[col].str.lower().str.contains(tag.lower())
                mask = mask.astype(bool).fillna(False)
                df = df_data[mask]
                df = df.groupby([df.date.dt.year, df.date.dt.month]).amount.sum()
                df_cat[(cat, tag)] = df
                df_data = df_data[~mask]  # remove to avoid double counting
        df_cat.loc[('Total', '')] = df_cat.sum()
        return df_cat.fillna(0)


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
        s = select(self.T.tags, self.T.category, TMeta.tag_type).join(self.T)
        df = pd.read_sql(s, Data.ENGINE)
        return df.set_index(['category', 'tag_type']).sort_index()
    v = view

    def write(self, data: dict):
        meta = self.meta.set_index('tag_type')
        df = pd.DataFrame(data).stack().explode().reset_index(level=0)
        df.columns = ['category', 'tags']
        df = df.join(meta).rename(columns={'id': 'meta_id'})
        df = df.sort_values(['meta_id', 'category'])
        return Data.write(df, self.T)

    def agg_lists(self) -> pd.Series:
        df = self.v
        return df.groupby(df.index.names)['tags'].agg(list)


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
