from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class TExcludeMeta(Base):
    __tablename__ = 'exclude_meta'

    id = Column(Integer, primary_key=True, autoincrement=True)
    column_name = Column(String, nullable=False, unique=True)

    exclude = relationship('TExclude', back_populates='exclude_meta', cascade='all, delete')


class TExclude(Base):
    __tablename__ = 'exclude'

    tags = Column(String, primary_key=True)
    exclude_id = Column(Integer, ForeignKey('exclude_meta.id'), nullable=False)

    exclude_meta = relationship('TExcludeMeta', back_populates='exclude')

