from sqlalchemy import Column, Integer, Text
from db import Base

class Character(Base):
    __tablename__ = "characters"
    __table_args__ = {"schema": "hp"}
    id = Column(Integer, primary_key=True, index=True)
    name = Column(Text, nullable=True)