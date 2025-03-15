from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class ExcludeMeta(Base):
    __tablename__ = 'exclude_meta'

    id = Column(Integer, primary_key=True)
    column_name = Column(String, nullable=False, unique=True)

    exclude = relationship('Exclude', back_populates='exclude_meta', cascade='all, delete')


class Exclude(Base):
    __tablename__ = 'exclude'

    tags = Column(String, primary_key=True)
    exclude_id = Column(Integer, ForeignKey('exclude_meta.id'), nullable=False)

    exclude_meta = relationship('ExcludeMeta', back_populates='exclude')

