import hashlib
from pathlib import Path

from sqlalchemy import (Column, Integer, String, ForeignKey, DateTime, func, Engine,
                        Numeric, UniqueConstraint, select, tuple_)
from sqlalchemy.orm import declarative_base, relationship, Session

Base = declarative_base()


class classproperty:  # noqa
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, instance, owner):
        return self.fget(owner)


class MyBase(Base):
    __abstract__ = True

    SORT_BY = [0]
    EXCLUDE_COLS = []

    @classproperty
    def name_(self):
        return f'T_{self.__tablename__.upper()}'

    @classproperty
    def columns_(self):
        return [c for c in self.__table__.columns if c.name not in self.EXCLUDE_COLS]

    @classmethod
    def delete(cls, s: Session, *clause, verbose=True):
        objs = s.query(cls).filter(*clause).all()
        for o in objs:
            s.delete(o)
        if verbose and len(objs) > 0:
            print(f'Removed {len(objs)} rows from {cls.name_}.')

    @classmethod
    def insert(cls, s: Session, objs: list['MyBase'], verbose=True) -> int:
        s.bulk_save_objects(objs)
        if verbose and len(objs) > 0:
            print(f'Inserted {len(objs)} rows into {cls.name_}.')
        return len(objs)

    @classmethod
    def drop(cls, engine: Engine):
        cls.__table__.drop(engine)

    @classmethod
    def existing(cls, s: Session) -> set:
        data = set(s.execute(select(*cls.columns_)).all())
        return {t[0] for t in data} if len(cls.columns_) == 1 else data

    @classmethod
    def read_file(cls, data: dict, s: Session) -> set:
        """ Read the data from file into a set"""
        return set(data)

    @classmethod
    def write(cls, s: Session, data: dict) -> int:
        """ Insert the data into the DB"""

        existing = cls.existing(s)
        from_file = cls.read_file(data, s)

        # remove categories not in file data
        to_remove = existing - from_file
        cols = cls.columns_
        cls.delete(s, tuple_(*cols).in_(to_remove))

        # Add new categories
        to_add = sorted(from_file - existing,
                        key=lambda x: tuple(x[i] for i in cls.SORT_BY))
        return cls.insert(s, [cls(**{c.name: val for c, val in zip(cols, vals)})
                              for vals in to_add])


class TData(MyBase):
    __tablename__ = 'data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    execution_date = Column(DateTime, nullable=False)
    title = Column(String, nullable=False)
    vendor = Column(String)
    account = Column(String)
    amount = Column(Numeric(10, 2), nullable=False)
    balance = Column(Numeric(12, 2), nullable=False)
    category = Column(String)
    sub_category = Column(String)

    __table_args__ = (UniqueConstraint('date', 'title', 'amount', 'balance',
                                       name='uix_date_tit_am_bal'),)


class TMeta(MyBase):
    __tablename__ = 'meta'
    EXCLUDE_COLS = ['id']

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_type = Column(String, nullable=False, unique=True)

    exclude = relationship('TExclude', back_populates='meta', cascade='all, delete')
    category = relationship('TTag', back_populates='meta', cascade='all, delete')

    @classmethod
    def read_file(cls, data: dict, s: Session) -> set:
        return set(c.name for c in TData.columns_) - {'category', 'sub_category'}


class TExclude(MyBase):
    __tablename__ = 'exclude'
    SORT_BY = [1, 0]

    tags = Column(String, primary_key=True)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    meta = relationship('TMeta', back_populates='exclude')

    @classmethod
    def read_file(cls, data: dict, s: Session) -> set:
        meta_map = {m.tag_type: m.id for m in s.scalars(select(TMeta)).all()}
        return {(tag.lower(), meta_map[tag_type.lower()])
                for tag_type, tags in data.items() for tag in tags}


class TCategory(MyBase):
    __tablename__ = 'categories'
    EXCLUDE_COLS = ['id']

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    subcategories = relationship('TSubCategory', back_populates='category',
                                 cascade='all, delete')


class TSubCategory(MyBase):
    __tablename__ = 'subcategories'
    SORT_BY = [1, 0]
    EXCLUDE_COLS = ['id']

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    category_id = Column(Integer, ForeignKey('categories.id'), nullable=False)

    category = relationship('TCategory', back_populates='subcategories')
    tags = relationship('TTag', back_populates='subcategory', cascade='all, delete')

    @classmethod
    def read_file(cls, data: dict, s: Session) -> set:
        cat = {i.name: i.id for i in s.scalars(select(TCategory)).all()}
        return set((sc, cat[n]) for n, subs in data.items() for sc in subs)


class TTag(MyBase):
    __tablename__ = 'tags'
    SORT_BY = [1, 2, 0]
    EXCLUDE_COLS = ['id']

    id = Column(Integer, primary_key=True)
    value = Column(String, nullable=False)
    subcategory_id = Column(Integer, ForeignKey('subcategories.id'), nullable=False)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    subcategory = relationship('TSubCategory', back_populates='tags')
    meta = relationship('TMeta', back_populates='category')

    @classmethod
    def read_file(cls, data: dict, s: Session) -> set:
        subcat = {sc.name: sc.id for sc in s.scalars(select(TSubCategory)).all()}
        meta = {m.tag_type: m.id for m in s.scalars(select(TMeta)).all()}
        return {(tag.lower(), subcat[sc], meta[m])
                for subs in data.values() for sc, td in subs.items()
                for m, tags in td.items() for tag in tags}


class TFileHash(MyBase):
    __tablename__ = 'file_hashes'

    fname = Column(String, primary_key=True)  # file path or identifier
    hash = Column(String, nullable=False)  # SHA256 hex digest
    time_stamp = Column(DateTime, default=func.now(), onupdate=func.now())

    @staticmethod
    def compute(path: Path) -> str:
        """Compute SHA256 hash of a file."""
        m = hashlib.sha256()
        m.update(path.read_bytes())
        return m.hexdigest()

    @classmethod
    def has_update(cls, session, file_path: Path) -> bool:
        """Return True if file is new/changed, False if unchanged."""
        new_hash = cls.compute(file_path)
        record = session.get(cls, str(file_path))
        return record is None or record.hash != new_hash

    @classmethod
    def write(cls, s: Session, path: Path):
        hash_ = cls.compute(path)
        record = s.get(cls, str(path))
        if record is None:
            record = cls(fname=str(path), hash=hash_)
            s.add(record)
        else:
            record.hash = hash_

    @classmethod
    def clean(cls, session, data_dir: Path):
        fnames = session.scalars(select(cls.fname)).all()
        fnames_new = [str(f) for f in data_dir.glob('*')]
        for fname in fnames:
            if fname not in fnames_new:
                cls.delete(session, cls.fname == fname)
