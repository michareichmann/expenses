from pathlib import Path
import json
import hashlib

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.tables import Base, TExclude, TExcludeMeta


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
        return self.read_db(Exclude)

    @staticmethod
    def insert2db(table: Base, *rows):
        df = pd.DataFrame(rows, columns=[col.name for col in table.__table__.columns])
        df.to_sql(table.__tablename__, Data.ENGINE, index=False, if_exists='append')

    @staticmethod
    def read_db(table: Base):
        return pd.read_sql(select(table), Data.ENGINE)


class Exclude:

    FNAME = Data.DIR / 'exclude.json'
    FNAME_HASH = Data.DIR / '.excl.hash'

    def __init__(self):
        self.has_update()

    @property
    def view(self):
        s = select(TExclude.tags, TExcludeMeta.column_name).join(TExclude, TExclude.exclude_id == TExcludeMeta.id).order_by('column_name')  # noqa
        return pd.read_sql(s, Data.ENGINE)
    v = view

    @property
    def t(self):
        return Data.read_db(TExclude)

    @property
    def meta(self):
        return Data.read_db(TExcludeMeta)

    @staticmethod
    def make_hash():
        m = hashlib.sha256()
        m.update(Exclude.FNAME.read_bytes())
        return m.hexdigest()

    @staticmethod
    def read_hash():
        if Exclude.FNAME_HASH.exists():
            return Exclude.FNAME_HASH.read_text()

    @staticmethod
    def has_update():
        old_hash = Exclude.read_hash()
        new_hash = Exclude.make_hash()
        if old_hash != new_hash:
            with open(Exclude.FNAME_HASH, 'w') as f:
                f.write(new_hash)
        return old_hash != new_hash

    @staticmethod
    def load_input():
        data = [(key.title(), value) for key, value in json.loads(Exclude.FNAME.read_text()).items()]
        return dict(sorted(data))

    def update(self):
        if not self.has_update():
            print('no update')
            return
        data = self.load_input()
        meta_cols = self.meta.column_name.values
        if len(data) != meta_cols.size or any(self.meta.column_name.values != list(data)):  # refill tables if the cols have changed
            print('write all')
            self.write_all(data)
        else:
            print('updating tags')
            self.update_tags(data)

    @staticmethod
    def del_all():
        Data.SESSION.query(TExcludeMeta).delete()
        Data.SESSION.query(TExclude).delete()
        Data.SESSION.commit()

    @staticmethod
    def write_all(data: dict):
        Exclude.del_all()
        Data.SESSION.bulk_save_objects([TExcludeMeta(column_name=col) for col in data])
        Data.SESSION.bulk_save_objects([TExclude(tags=tags, exclude_id=id_) for id_, lst in enumerate(data.values(), 1) for tags in lst])
        Data.SESSION.commit()

    def update_tags(self, data: dict):
        meta = self.meta.set_index('column_name')['id']
        old_data = self.view
        new_data = pd.DataFrame([(tags, key) for key, lst in data.items() for tags in lst], columns=old_data.columns)
        df = new_data.merge(old_data, how='outer', indicator=True)
        # delete all rows which are not in the new data anymore
        for _, row in df[df['_merge'] == 'right_only'].iterrows():
            Data.SESSION.query(TExclude).filter_by(tags=row.tags, exclude_id=int(meta.at[row.column_name])).delete()
        # add new tags
        for _, row in df[df['_merge'] == 'left_only'].iterrows():
            Data.SESSION.add(TExclude(tags=row.tags, exclude_id=int(meta.at[row.column_name])))
        Data.SESSION.commit()

    def rename_col(self, old_col, new_col):
        raise NotImplementedError('cmon!')
