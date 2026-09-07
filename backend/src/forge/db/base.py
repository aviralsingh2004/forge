from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Base class for all Forge SQLAlchemy models."""

    metadata = MetaData()
