import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Type, Iterable

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.tables import Base, TMeta, TExclude, TCategory, MyBase, TFileHash, TData


# TODO: revise category structure: cat, sub_cat and then tags

class Data(pd.DataFrame):

    DIR = Path(__file__).resolve().parent.parent / 'data'
    DB_PATH = DIR / 'data.db'

    ENGINE = create_engine('sqlite:///example.db', echo=False)
    Base.metadata.create_all(ENGINE)

    _Session = sessionmaker(bind=ENGINE)
    SESSION = _Session()

    cat, excl = [None] * 2

    def __init__(self, data=None, force_update=False, **kwargs):
        if data is None:
            data = self.load()
        super().__init__(data, **kwargs)

        self.cat = Categories()
        self.excl = Exclude()
        self.update_db(force_update)

    # --------------------------------------------
    # region INIT
    def load(self):
        try:
            return self.read(TData).set_index('id')
        except Exception as err:
            print(f'could not read {TData.name} from DB: {err}')
            return pd.DataFrame(columns=self.orm_cols)

    def read_csv(self, fname: Path):
        cols = [col for col in self.orm_cols if col.lower() != 'id']
        date_cols = [col for col in cols if 'date' in col]
        return pd.read_csv(fname, names=cols, skiprows=1, usecols=range(7),
                           parse_dates=date_cols, dayfirst=True, decimal=',')

    def files_to_update(self, all_=False):
        x = [f for f in self.fnames if all_ or TFileHash.has_update(self.SESSION, f)]
        return sorted(x, key=lambda f: f.stat().st_ctime)

    def update_db(self, force=False):
        fnames = self.files_to_update(force)
        if len(fnames) > 0:
            df_in = pd.concat([self.read_csv(f) for f in fnames]).drop_duplicates()
            if len(self):
                aux_cols = ['category', 'sub_category']
                df_new = pd.concat([self.drop(columns=aux_cols), df_in])
                # keep only the rows which are not duplicated (not in the DB)
                df_new = df_new.drop_duplicates(keep=False)
            else:
                df_new = df_in
            self.write(df_new)
            n0, n1 = len(self), len(df_new)
            self[:] = self.load()
            print(f'inserted {n1} rows into {TData.name} ({n0} -> {n0 + n1})')
        ret_ex = self.update_excluded(force)
        ret_cat = self.update_categories(force)
        if ret_ex == 0 or ret_cat == 0:
            self[:] = self.load()
        return ret_ex + ret_cat + (0 if len(fnames) else -1)

    def update_excluded(self, force=False):
        if not self.excl.was_updated and not force:
            return -1
        df = self.excl.v.pivot(columns='tag_type')
        df.columns = df.columns.droplevel(0)
        masks = [self.contains(col, df[col].dropna()) for col in df.columns]
        mask = pd.concat(masks, axis=1).any(axis=1)
        ids = mask[mask].index.tolist()
        self.SESSION.query(TData).filter(TData.id.in_(ids)).update(
            {TData.category: 'excluded'}, synchronize_session=False)
        self.SESSION.commit()
        print(f'excluded {len(ids)} rows from {TData.name}')
        return 0

    def update_categories(self, force=False):
        """Assigns category and sub_category to the data based on tag matching."""
        if not self.cat.was_updated and not force:
            return -1
        df = self.load()
        changed_rows = 0
        for (cat, col), tags in self.cat.agg_lists().items():
            for tag in tags:
                mask = df[col].str.lower().str.contains(tag.lower(), na=False)
                assign_mask = mask & df['category'].isna()  # don't overwrite existing
                ids = df[assign_mask].index.tolist()
                if ids:
                    changed_rows += len(ids)
                    self.SESSION.query(TData).filter(TData.id.in_(ids)).update(
                        {TData.category: cat, TData.sub_category: tag},
                        synchronize_session=False)
        self.SESSION.commit()
        print(f'updated {changed_rows} categories in {TData.name}')
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

    @property
    def excluded(self):
        drop_cols = ['category', 'sub_category']
        return self[self.category == 'excluded'].drop(columns=drop_cols)
    # endregion
    # --------------------------------------------

    def _write_meta(self):
        """ update the Meta table.
        only use if the structure of the input data changed. """
        new = [col for col in self.orm_cols if 'category' not in col]
        TMeta.delete(self.SESSION)
        Data.SESSION.bulk_save_objects([TMeta(tag_type=typ) for typ in new])
        Data.SESSION.commit()
        print(f'updated {TMeta.name} with {len(new)} rows')

    def contains(self, col: str, lst: Iterable):
        or_str = '|'.join(x.lower() for x in lst)
        return self[col].str.lower().str.contains(or_str).astype(bool).fillna(False)

    def categorise(self, show_sub_cat=False, show_month=False):
        cat_cols = ['category', 'sub_category'] if show_sub_cat else ['category']
        date_cols = [self.date.dt.year] + ([self.date.dt.month] if show_month else [])
        df = self.groupby(date_cols + cat_cols)[['amount']].sum()
        date_names = ['year'] + (['month'] if show_month else [])
        df = df.unstack(cat_cols).sort_index(axis=1).rename_axis(date_names)
        df.loc[('total', '') if show_month else 'total', :] = df.sum()
        return df.style.format('{:,.2f}', na_rep='')
        # .background_gradient(cmap='Blues', axis=1)

    def show_uncategorised(self):
        drop_cols = ['category', 'sub_category']
        return self[self.category.isna()].drop(columns=drop_cols)


class _Base(ABC):
    FNAME: Path
    T: Type[MyBase]

    def __init__(self):
        self.was_updated = self.update()

    @property
    def table(self):
        return Data.read(self.T)
    t = table

    @property
    def meta(self):
        return Data.read(TMeta)

    @abstractmethod
    def view(self):
        pass

    @classmethod
    def has_update(cls):
        return TFileHash.has_update(Data.SESSION, cls.FNAME)

    @classmethod
    def write_hash(cls):
        TFileHash.write(Data.SESSION, cls.FNAME)

    def update(self, force=False) -> bool:
        if force or self.has_update():
            self.write_hash()
            return self.write() > 0
        return False

    def read_json(self):
        return json.loads(self.FNAME.read_text())

    @abstractmethod
    def write(self) -> int:
        """ Insert the data of the main table into the DB"""


class Categories(_Base):
    FNAME = Data.DIR / 'categories.json'
    T = TTag

    @property
    def view(self):
        s = select(TCategory.name.label('category'),
                   TSubCategory.name.label('sub_category'),
                   TMeta.tag_type, TTag.value.label('tag')).select_from(TTag).join(
            TSubCategory).join(TCategory).join(TMeta)
        df = pd.read_sql(s, Data.ENGINE)
        return df.set_index(['category', 'sub_category', 'tag_type']).sort_index()
    v = view

    def write(self):
        data = self.read_json()
        n = TCategory.write(Data.SESSION, data)
        n += TSubCategory.write(Data.SESSION, data)
        return n + TTag.write(Data.SESSION, data)

    def agg_lists(self) -> pd.Series:
        df = self.v
        return df.groupby(df.index.names)['tag'].agg(list)


class Exclude(_Base):
    FNAME = Data.DIR / 'exclude.json'
    T = TExclude

    def write(self) -> int:
        return self.T.write(Data.SESSION, self.read_json())

    @property
    def view(self):
        s = select(self.T.tags, TMeta.tag_type).join(self.T).order_by('tag_type')
        return pd.read_sql(s, Data.ENGINE)
    v = view
