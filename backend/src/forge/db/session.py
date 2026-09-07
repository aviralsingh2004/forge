from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from forge.db.config import get_settings


def create_engine():
    """Create the async SQLAlchemy engine from environment-backed settings."""

    return create_async_engine(get_settings().async_database_url, pool_pre_ping=True)


engine = create_engine()
AsyncSessionFactory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionFactory() as session:
        yield session
