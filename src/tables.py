import hashlib
from pathlib import Path

from sqlalchemy import (Column, Integer, String, ForeignKey, DateTime, func, Engine,
                        Numeric, UniqueConstraint, select)
from sqlalchemy.orm import declarative_base, relationship, Session

Base = declarative_base()


class classproperty:  # noqa
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, instance, owner):
        return self.fget(owner)


class MyBase(Base):
    __abstract__ = True

    @classproperty
    def name_(self):
        return f'T_{self.__tablename__.upper()}'

    @classmethod
    def delete(cls, s: Session, *clause, commit=True, verbose=True):
        objs = s.query(cls).filter(*clause).all()
        for o in objs:
            s.delete(o)
        if verbose:
            print(f'Removed {len(objs)} rows from {cls.name_}.')
        if commit:
            s.commit()

    @classmethod
    def insert(cls, s: Session, objs: list['MyBase'], commit=True, verbose=True):
        s.bulk_save_objects(objs)
        if verbose:
            print(f'Inserted {len(objs)} rows into {cls.name_}.')
        if commit:
            s.commit()

    @classmethod
    def drop(cls, engine: Engine):
        cls.__table__.drop(engine)


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

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_type = Column(String, nullable=False, unique=True)

    exclude = relationship('TExclude', back_populates='meta', cascade='all, delete')
    category = relationship('TTag', back_populates='meta', cascade='all, delete')


class TExclude(MyBase):
    __tablename__ = 'exclude'

    tags = Column(String, primary_key=True)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    meta = relationship('TMeta', back_populates='exclude')


class TCategory(MyBase):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    subcategories = relationship('TSubCategory', back_populates='category',
                                 cascade='all, delete')

    @classmethod
    def write(cls, s: Session, data: list):
        """ Insert the categories into the DB"""
        existing = {c.name for c in s.query(cls.name).all()}
        new = set(data)

        # remove categories not in new data
        to_remove = existing - new
        cls.delete(s, cls.name.in_(to_remove), commit=False)

        # Add new categories
        to_add = new - existing
        cls.insert(s, [cls(name=cat) for cat in to_add])


class TSubCategory(MyBase):
    __tablename__ = 'subcategories'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    category_id = Column(Integer, ForeignKey('categories.id'), nullable=False)
    category = relationship('TCategory', back_populates='subcategories')
    tags = relationship('TTag', back_populates='subcategory', cascade='all, delete')


class TTag(MyBase):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    value = Column(String, nullable=False)
    subcategory_id = Column(Integer, ForeignKey('subcategories.id'), nullable=False)
    subcategory = relationship('TSubCategory', back_populates='tags')
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)
    meta = relationship('TMeta', back_populates='category')


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

        if record is not None and record.hash == new_hash:
            return False

        if record is None:  # first time seen
            record = cls(fname=str(file_path), hash=new_hash)
            session.add(record)

        elif record.hash != new_hash:
            record.hash = new_hash

        session.commit()
        return True

    @classmethod
    def clean(cls, session, data_dir: Path):
        fnames = session.scalars(select(cls.fname)).all()
        fnames_new = [str(f) for f in data_dir.glob('*')]
        for fname in fnames:
            if fname not in fnames_new:
                cls.delete(session, cls.fname == fname, commit=False)
        session.commit()
