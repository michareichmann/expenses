import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Type
import pandas as pd
from src.db import get_session, read_table, read_sql, select
from sqlalchemy.orm import Session
from src.utils import DATA_DIR
from src.tables import (TMeta, TExclude, TCategory, MyBase, TFileHash, TSubCategory,
                        TTag, TData)


class _Base(ABC):
    FNAME: Path
    T: Type[MyBase]

    def __init__(self):
        self.was_updated = self.update()

    @property
    def table(self):
        return read_table(self.T)
    t = table

    @property
    def meta(self):
        return read_table(TMeta)

    @abstractmethod
    def view(self):
        pass

    @classmethod
    def has_update(cls, s: Session):
        return TFileHash.has_update(s, cls.FNAME)

    @classmethod
    def write_hash(cls, s: Session):
        TFileHash.write(s, cls.FNAME)

    @classmethod
    def update(cls, force=False) -> bool:
        with get_session() as s:
            if force or cls.has_update(s):
                cls.write_hash(s)
                return cls.write(s) > 0
            return False

    @classmethod
    def read_json(cls):
        return json.loads(cls.FNAME.read_text())

    @classmethod
    @abstractmethod
    def write(cls, s: Session) -> int:
        """ Insert the data of the main table into the DB"""


class Categories(_Base):
    FNAME = DATA_DIR / 'categories.json'
    T = TTag

    @property
    def view(self):
        s = select(TCategory.name.label('category'),
                   TSubCategory.name.label('sub_category'),
                   TMeta.tag_type, TTag.value.label('tag')).select_from(TTag).join(
            TSubCategory).join(TCategory).join(TMeta)
        df = read_sql(s)
        return df.set_index(['category', 'sub_category', 'tag_type']).sort_index()
    v = view

    @classmethod
    def write(cls, s: Session) -> int:
        data = cls.read_json()
        n = TCategory.write(s, data)
        n += TSubCategory.write(s, data)
        return n + TTag.write(s, data)

    def agg_lists(self) -> pd.Series:
        df = self.v
        df = df.groupby(df.index.names)['tag'].agg(list)
        # sort by tag_type according to TData.TYPE_ORDER
        sort_indices = df.index.get_level_values(2).map(
            lambda x: TData.TYPE_ORDER.index(x)).argsort()
        return df.iloc[sort_indices]


class Exclude(_Base):
    FNAME = DATA_DIR / 'exclude.json'
    T = TExclude

    @classmethod
    def write(cls, s: Session) -> int:
        return cls.T.write(s, cls.read_json())

    @property
    def view(self):
        s = select(self.T.tags, TMeta.tag_type).join(self.T).order_by('tag_type')
        return read_sql(s)
    v = view
