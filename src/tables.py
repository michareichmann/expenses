from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class TMeta(Base):
    __tablename__ = 'meta'

    id = Column(Integer, primary_key=True, autoincrement=True)
    tag_type = Column(String, nullable=False, unique=True)

    exclude = relationship('TExclude', back_populates='meta', cascade='all, delete')
    category = relationship('TCategory', back_populates='meta', cascade='all, delete')


class TExclude(Base):
    __tablename__ = 'exclude'

    tags = Column(String, primary_key=True)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    meta = relationship('TMeta', back_populates='exclude')


class TCategory(Base):
    __tablename__ = 'category'

    category = Column(String, primary_key=True)
    tags = Column(String, primary_key=True)
    meta_id = Column(Integer, ForeignKey('meta.id'), nullable=False)

    meta = relationship('TMeta', back_populates='category')
