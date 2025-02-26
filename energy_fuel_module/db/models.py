from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class State(Base):
    ''' Create a statemap to match States table in DB'''
    __tablename__ = "states"

    state_id = Column(Integer, primary_key=True, autoincrement=True)
    state_code = Column(String(2), unique=True, nullable=False)
    state_name = Column(String(50), nullable=False)
    region = Column(String(20), nullable=False)