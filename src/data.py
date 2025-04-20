from pathlib import Path
import json
import hashlib

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.tables import Base, TMeta, TExclude, TCategory


# todo: category, data tables
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
        return self.read_db(TExclude)

    @staticmethod
    def insert2db(table: Base, *rows):
        df = pd.DataFrame(rows, columns=[col.name for col in table.__table__.columns])
        df.to_sql(table.__tablename__, Data.ENGINE, index=False, if_exists='append')

    @staticmethod
    def read_db(table: Base):
        return pd.read_sql(select(table), Data.ENGINE)


class _Base:

    FNAME = None
    FNAME_HASH = None
    T = None
    META_ID_NAME = None

    def __init__(self):
        self.Hash = self.get_file_hash()
        self.update()

    @property
    def t(self):
        return Data.read_db(self.T)

    @property
    def meta(self):
        return Data.read_db(TMeta)

    @property
    def view(self):
        s = select(self.T.tags, TMeta.tag_type).join(self.T).order_by('tag_type')
        return pd.read_sql(s, Data.ENGINE)
    v = view

    def get_file_hash(self):
        m = hashlib.sha256()
        m.update(self.FNAME.read_bytes())
        return m.hexdigest()

    def save_hash(self):
        with open(self.FNAME_HASH, 'w') as f:
            f.write(self.Hash)

    def del_all(self):
        Data.SESSION.query(TMeta).delete()
        Data.SESSION.query(self.T).delete()
        Data.SESSION.commit()

    def has_update(self):
        return self.read_old_hash() != self.Hash

    def update(self):
        if not self.has_update():
            print('no update')
            return
        data = self.load_input()
        meta_cols = self.meta.tag_type.values
        if len(data) != meta_cols.size or any(self.meta.tag_type.values != list(data)):  # refill tables if the cols have changed
            print('write all')
            self.write_all(data)
        else:
            print('updating tags')
            self.update_tags(data)
        self.save_hash()

    def read_old_hash(self):
        if self.FNAME_HASH.exists():
            return self.FNAME_HASH.read_text()

    def load_input(self):
        data = [(key.title(), value) for key, value in json.loads(self.FNAME.read_text()).items()]
        return dict(sorted(data))

    def write_all(self, data: dict):
        raise NotImplementedError()

    def update_tags(self, data):
        raise NotImplementedError()


class Categories(_Base):

    FNAME = Data.DIR / 'categories.json'
    FNAME_HASH = Data.DIR / '.excl.hash'

    T = TCategory

    @property
    def view(self):
        s = select(self.T.tags, self.T.category, TMeta.tag_type).join(self.T).order_by('tag_type')
        df = pd.read_sql(s, Data.ENGINE)
        return df.sort_values(['category', 'tag_type']).set_index(['category', 'tag_type'])
    v = view

    def update(self):
        return

    def write_all(self, data: dict):
        self.del_all()
        Data.SESSION.bulk_save_objects([TMeta(tag_type=col) for col in data])
        rows = [TCategory(tags=tags, category=cat, meta_id=i) for i, (col, dic) in enumerate(data.items(), 1) for cat, lst in dic.items() for tags in lst]
        Data.SESSION.bulk_save_objects(rows)
        Data.SESSION.commit()


class Exclude(_Base):

    FNAME = Data.DIR / 'exclude.json'
    FNAME_HASH = Data.DIR / '.excl.hash'

    T = TExclude

    def write_all(self, data: dict):
        # TODO: update tags should be enough here (exclude meta must be subset of cat meta)
        self.del_all()
        Data.SESSION.bulk_save_objects([TMeta(tag_type=col) for col in data])
        Data.SESSION.bulk_save_objects([TExclude(tags=tags, meta_id=id_) for id_, lst in enumerate(data.values(), 1) for tags in lst])
        Data.SESSION.commit()

    def update_tags(self, data: dict):
        meta = self.meta.set_index('tag_type')['id']
        old_data = self.view
        new_data = pd.DataFrame([(tags, key) for key, lst in data.items() for tags in lst], columns=old_data.columns)
        df = new_data.merge(old_data, how='outer', indicator=True)
        # delete all rows which are not in the new data anymore
        for _, row in df[df['_merge'] == 'right_only'].iterrows():
            Data.SESSION.query(TExclude).filter_by(tags=row.tags, meta_id=int(meta.at[row.tag_type])).delete()
        # add new tags
        for _, row in df[df['_merge'] == 'left_only'].iterrows():
            Data.SESSION.add(TExclude(tags=row.tags, meta_id=int(meta.at[row.tag_type])))
        Data.SESSION.commit()

    def rename_col(self, old_col, new_col):
        raise NotImplementedError('cmon!')
