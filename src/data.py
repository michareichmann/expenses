from pathlib import Path
import json

import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from src.tables import Base, Exclude, ExcludeMeta


# todo: category, data tables
# todo: check if exclude file has changed and then update DB
# todo: handle duplicates (see crypto)
# todo: exclude view check the relationship from sqlalchemy

class Data(pd.DataFrame):

    DIR = Path(__file__).resolve().parent.parent / 'data'
    DB_PATH = DIR / 'data.db'

    ENGINE = create_engine("sqlite:///example.db", echo=False)
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
    def init_v_excl():
        return f"""
            CREATE VIEW {Data.V_EXCL} AS
                SELECT 
                    TAGS, COLUMN_NAME
                FROM {Data.T_EXCL} e
                LEFT JOIN {Data.T_EXCL_META} m ON e.EXCLUDE_ID = m.ID
        """

    @staticmethod
    def insert_exclude():
        f = Data.DIR / 'exclude.json'
        data = json.loads(f.read_text())
        # m = hashlib.sha256()
        # m.update(f.read_bytes())
        # m.hexdigest()
        Data.insert2db(ExcludeMeta, *[(i, col) for i, col in enumerate(data.keys())])
        Data.insert2db(Exclude, *[(tag, i) for i, lst in enumerate(data.values()) for tag in lst])

    @staticmethod
    def insert2db(table: Base, *rows):
        df = pd.DataFrame(rows, columns=[col.name for col in table.__table__.columns])
        df.to_sql(table.__tablename__, Data.ENGINE, index=False, if_exists='append')

    @staticmethod
    def read_db(table: Base):
        return pd.read_sql(select(table), Data.ENGINE)


