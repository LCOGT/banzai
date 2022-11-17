from typing import AsyncGenerator
from os import environ

from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection

from .. import dbs

DB_ADDRESS = environ.get("DB_ADDRESS", "sqlite+aiosqlite:///./test.db")

engine = create_async_engine(DB_ADDRESS, echo=True)

async def init_models():
    async with engine.begin() as conn:
        await conn.run_sync(dbs.Base.metadata.create_all)


async def begin_conn() -> AsyncGenerator[AsyncConnection, None]:
    async with engine.begin() as conn:
        yield  conn
