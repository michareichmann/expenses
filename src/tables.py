import hashlib
from pathlib import Path

from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, func, delete, Engine
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class classproperty:  # noqa
    def __init__(self, fget):
        self.fget = fget

    def __get__(self, instance, owner):
        return self.fget(owner)


class MyBase(Base):
    __abstract__ = True

    @classproperty
    def name(self):
        return f'T_{self.__tablename__.upper()}'

    @classmethod
    def delete(cls, session, *clause, commit=True):
        session.execute(delete(cls).where(*clause))
        if commit:
            session.commit()

    @classmethod
    def drop(cls, engine: Engine):
        cls.__table__.drop(engine)

class TMeta(MyBase):
    __tablename__ = 'meta'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_type = Column(String, nullable=False, unique=True)

    exclude = relationship('TExclude', back_populates='meta', cascade='all, delete')
    category = relationship('TCategory', back_populates='meta', cascade='all, delete')


class TExclude(MyBase):
    __tablename__ = 'exclude'

    tags = Column(String, primary_key=True)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    meta = relationship('TMeta', back_populates='exclude')


class TCategory(MyBase):
    __tablename__ = 'category'

    category = Column(String, primary_key=True)
    tags = Column(String, primary_key=True)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    meta = relationship('TMeta', back_populates='category')


class TFileHash(MyBase):
    __tablename__ = 'file_hashes'

    fname = Column(String, primary_key=True)     # file path or identifier
    hash = Column(String, nullable=False)        # SHA256 hex digest
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
