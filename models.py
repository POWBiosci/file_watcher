import typing as ty
from datetime import datetime
from sqlalchemy import create_engine, insert
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import String, JSON, TIMESTAMP, func
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column 

class Base(DeclarativeBase):
    pass

class MetaData(Base):
    """
    Represents a table of uploaded metadata files

    Attributes:
        `filename` (str): Name of file
        `created` (datetime): Timestamp indicating when the entry was created.
        `updated` (datetime): Timestamp indicating the last time the entry was updated.
    """

    __tablename__ = "metadata_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    filename: Mapped[str] = mapped_column(String)
    content: Mapped[ty.Dict[str,ty.Any]] = mapped_column(JSON)
    created: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True),default=func.now())
    updated: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True),onupdate=func.now())