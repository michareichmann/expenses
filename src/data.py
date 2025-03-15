import sqlite3
from pathlib import Path

import pandas as pd


class Data(pd.DataFrame):

    TABLE = 'T_TRANSACTIONS'
    T_EXCL_META = 'T_EXCLUDE_META'
    T_EXCL = 'T_EXCLUDE'
    V_EXCL = 'V_EXCLUDE'

    DIR = Path(__file__).resolve().parent.parent / 'data'
    DB_PATH = DIR / 'data.db'

    CONNECTION = sqlite3.connect(DB_PATH)
    CURSOR = CONNECTION.cursor()

    def __init__(self, data=None, **kwargs):
        if data is None:
            self.init_db()  # create new table if it does not exist
            data = self.load()
        super().__init__(data, **kwargs)

    # --------------------------------------------
    # region INIT
    def load(self):
        return Data.read_exclude()

    @staticmethod
    def init_db():
        for f in [Data.init_exclude_meta, Data.init_exclude, Data.init_v_excl]:
            Data.CONNECTION.cursor().execute(f())

        # Data.CONNECTION.cursor().execute(f"""
        # CREATE TABLE IF NOT EXISTS {Data.TABLE} (
        #     ID INTEGER PRIMARY KEY,
        #     Symbol TEXT NOT NULL,
        #     Type TEXT NOT NULL,
        #     Quantity FLOAT NOT NULL,
        #     Q_NET FLOAT NOT NULL,
        #     Price FLOAT,
        #     Value FLOAT,
        #     Fees FLOAT,
        #     Currency TEXT,
        #     Date DATETIME NOT NULL,
        #     UNIQUE(Symbol, Date) -- Ensures the entire row is unique
        # )
        # """)

    @staticmethod
    def init_exclude_meta():
        return f"""
            CREATE TABLE IF NOT EXISTS {Data.T_EXCL_META} (
                ID INT NOT NULL,
                COLUMN_NAME TEXT NOT NULL
            )
            """

    @staticmethod
    def init_exclude():
        return f"""
            CREATE TABLE IF NOT EXISTS {Data.T_EXCL} (
                TAGS TEXT NOT NULL,
                EXCLUDE_ID INT NOT NULL,
                UNIQUE (TAGS, EXCLUDE_ID),
                FOREIGN KEY (EXCLUDE_ID) REFERENCES {Data.T_EXCL_META}(ID) ON DELETE CASCADE
            )
            """

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
    def insert2db(table, columns, *rows):
        df = pd.DataFrame(rows, columns=columns)
        df.to_sql(table, Data.CONNECTION, index=False, if_exists='append')

    @staticmethod
    def read_db(table):
        return pd.read_sql(f'SELECT * FROM {table}', Data.CONNECTION)

    @staticmethod
    def insert_exclude(*rows):
        Data.insert2db(Data.T_EXCL, ['TAGS', 'EXCLUDE_ID'], *rows)

    @staticmethod
    def insert_exclude_id(*rows):
        Data.insert2db(Data.T_EXCL_META, ['ID', 'COLUMN_NAME'], *rows)

    @staticmethod
    def read_exclude():
        return Data.read_db(Data.T_EXCL)

    # todo: exclude, category, data tables
