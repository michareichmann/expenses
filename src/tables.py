from sqlalchemy import Column, Integer, String, ForeignKey
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
        return self.__tablename__

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
