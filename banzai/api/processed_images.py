from fastapi import APIRouter, Depends
from fastapi_pagination import Page, add_pagination
from fastapi_pagination.ext.async_sqlalchemy import paginate
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection

from .. import dbs
from .db import begin_conn
from .schema import (
    ProcessedImage,
)


router = APIRouter(
    prefix="/processed-images",
    tags=["processed-images"],
)

@router.get("/", response_model=Page[ProcessedImage])
async def get_processed_images(
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.ProcessedImage).order_by(dbs.ProcessedImage.id.desc())
    return await paginate(conn, q)


add_pagination(router)
