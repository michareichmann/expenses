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

    @classproperty
    def name_(self):
        return f'T_{self.__tablename__.upper()}'

    @classmethod
    def delete(cls, s: Session, *clause, commit=True, verbose=True):
        objs = s.query(cls).filter(*clause).all()
        for o in objs:
            s.delete(o)
        if verbose and len(objs) > 0:
            print(f'Removed {len(objs)} rows from {cls.name_}.')
        if commit:
            s.commit()

    @classmethod
    def insert(cls, s: Session, objs: list['MyBase'], commit=True, verbose=True):
        s.bulk_save_objects(objs)
        if verbose and len(objs) > 0:
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
        from_file = set(data)

        # remove categories not in file data
        to_remove = existing - from_file
        cls.delete(s, cls.name.in_(to_remove), commit=False)

        # Add new categories
        to_add = sorted(from_file - existing)
        cls.insert(s, [cls(name=cat) for cat in to_add])


class TSubCategory(MyBase):
    __tablename__ = 'subcategories'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    category_id = Column(Integer, ForeignKey('categories.id'), nullable=False)

    category = relationship('TCategory', back_populates='subcategories')
    tags = relationship('TTag', back_populates='subcategory', cascade='all, delete')

    @classmethod
    def write(cls, s: Session, data: dict):
        """ Insert the subcategories into the DB"""
        existing = {(sc.name, sc.category_id)
                    for sc in s.query(cls.name, cls.category_id).all()}
        cat = {i.name: i.id for i in s.scalars(select(TCategory)).all()}
        from_file = set((sc, cat[n]) for n, subs in data.items() for sc in subs)

        # remove subcategories not in file data
        to_remove = existing - from_file
        cls.delete(s, tuple_(cls.name, cls.category_id).in_(to_remove), commit=False)

        # Add new subcategories
        to_add = sorted(from_file - existing, key=lambda x: (x[1], x[0]))
        objs = [cls(name=subcat, category_id=cat_id) for subcat, cat_id in to_add]
        cls.insert(s, objs)


class TTag(MyBase):
    __tablename__ = 'tags'

    id = Column(Integer, primary_key=True)
    value = Column(String, nullable=False)
    subcategory_id = Column(Integer, ForeignKey('subcategories.id'), nullable=False)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    subcategory = relationship('TSubCategory', back_populates='tags')
    meta = relationship('TMeta', back_populates='category')

    @classmethod
    def write(cls, s: Session, data: dict):
        """ Insert the tags into the DB"""
        existing = {(t.value, t.subcategory_id, t.meta_id)
                    for t in s.scalars(select(cls)).all()}

        subcat = {sc.name: sc.id for sc in s.scalars(select(TSubCategory)).all()}
        meta = {m.tag_type: m.id for m in s.scalars(select(TMeta)).all()}
        from_file = {(tag.lower(), subcat[sc], meta[m])
                     for subs in data.values()
                     for sc, td in subs.items()
                     for m, tags in td.items()
                     for tag in tags}

        # remove tags not in file data
        to_remove = existing - from_file
        cls.delete(s, tuple_(
            cls.value, cls.subcategory_id, cls.meta_id).in_(to_remove), commit=False)

        # Add new tags
        to_add = sorted(from_file - existing, key=lambda x: (x[1], x[2], x[0]))
        objs = [cls(value=value, subcategory_id=sc_id, meta_id=meta_id)
                for value, sc_id, meta_id in to_add]
        cls.insert(s, objs)


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
