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

    T = TData
    DIR: Path = DATA_DIR
    AUX_COLS = ['category', 'sub_category']

    def __init__(self, data=None, force_update=False, **kwargs):
        if data is None:
            data = self.read_from_db()
        super().__init__(data, **kwargs)

        self.cat = Categories()
        self.log = setup_logger(__name__)
        self.update_(force_update)

    # --------------------------------------------
    # region GETTERS
    @property
    def fnames(self):
        return list(self.DIR.glob('hist*.csv'))

    @property
    def min_date(self):
        return self.date.min()

    @property
    def max_date(self):
        return self.date.max()

    @property
    def excluded(self):
        return self[self.category == 'Exclude'].drop(columns=self.AUX_COLS)

    @property
    def n_excluded(self):
        return (self.category == 'Exclude').sum()
    # endregion GETTERS
    # --------------------------------------------

    # --------------------------------------------
    # region INIT & UPDATE
    def write(self, s: Session, df: pd.DataFrame):
        n0, n1 = len(self), len(df)
        self.log.info(f'inserted {n1} rows into {TData.name_} ({n0} -> {n0 + n1})')
        df.to_sql(self.T.__tablename__, s.bind, if_exists='append', index=False)
        return n1

    @staticmethod
    def read_from_db():
        try:
            return read_table(TData).set_index('id')
        except Exception as err:
            print(f'could not read {TData.name_} from DB: {err}')
            return pd.DataFrame(columns=TData.columns_)

    @staticmethod
    def read_csv(fname: Path):
        cols = [col for col in TData.column_names if col.lower() != 'id']
        date_cols = [col for col in cols if 'date' in col]
        return pd.read_csv(fname, names=cols, skiprows=1, usecols=range(7),
                           parse_dates=date_cols, dayfirst=True, decimal=',')

    def files_to_update(self, s: Session, update_all=False):
        x = [f for f in self.fnames if update_all or TFileHash.has_update(s, f)]
        return sorted(x)

    def update_(self, force=False):
        with get_session() as s:
            hist = self.update_history(s, force)
            cat = self.update_categories(s, force)
        if hist > 0 or cat > 0:
            self[:] = self.read_from_db()

    def update_history(self, s: Session, force=False):
        fnames = self.files_to_update(s, force)
        if len(fnames) == 0:
            return -1

        df_in = pd.concat([self.read_csv(f) for f in fnames]).drop_duplicates()
        for f in fnames:
            TFileHash.write(s, f)
        if len(self):
            aux_cols = ['category', 'sub_category']
            df_new = pd.concat([self.drop(columns=aux_cols), df_in])
            # keep only the rows which are not duplicated (not in the DB)
            df_new = df_new.drop_duplicates(keep=False)
        else:
            df_new = df_in
        if df_new.empty:
            return 0
        return self.write(s, df_new.sort_values('date'))

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
        print(f'excluded {len(ids)} rows from {TData.name_}')
        return 0

    def filter_allowed_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        fname = self.DIR / 'allowed_duplicates.json'
        data = json.loads(fname.read_text())
        data = {k.lower(): [w.lower() for w in v] for k, v in data.items()}
        unknown_types = set(data) - set(TData.TYPE_ORDER)
        assert len(unknown_types) == 0, (f'invalid types in allowed duplicates: '
                                         f'{unknown_types}')
        for tag_type, tags in data.items():
            pattern = '|'.join(tags)
            mask = df[tag_type].str.lower().str.contains(pattern, na=False, regex=True)
            df.loc[mask, 'n_matches'] = df.loc[mask, 'n_matches'].clip(upper=1)
        return df

    def match_categories(self, overwrite=False) -> pd.DataFrame:
        df = self.copy()
        if not overwrite:
            df = df[df.category.isna()]
        df['n_matches'] = 0
        for (cat, sub_cat, tag_type), tags in self.cat.agg_lists().items():
            pattern = '|'.join(tags)
            mask = df[tag_type].str.lower().str.contains(pattern, na=False, regex=True)
            df.loc[mask, ['category', 'sub_category']] = [cat, sub_cat]
            df.loc[mask, 'n_matches'] += 1
        return df

    def update_categories(self, s: Session, force=False, overwrite=False):
        if not self.cat.was_updated and not force:
            return -1
        df = self.match_categories(overwrite)
        df = self.filter_allowed_duplicates(df)
        df_upd = df[~df.category.isna()]
        if not df_upd.empty:
            counts = df.n_matches.value_counts()
            if (counts.index > 1).any():
                self.log.warning(f'{counts[counts.index > 1].sum()} '
                                 f'rows matched multiple tags')
            for idx, row in df_upd.iterrows():
                s.query(TData).filter(TData.id == idx).update({
                    TData.category: row.category,
                    TData.sub_category: row.sub_category},
                    synchronize_session=False)
            self.log.info(f'updated category of {len(df_upd)} rows in {TData.name_}')
            return len(df_upd)
        return 0
    # endregion INIT & UPDATE
    # --------------------------------------------

    # endregion
    # --------------------------------------------

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
