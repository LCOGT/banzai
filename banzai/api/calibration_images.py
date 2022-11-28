from typing import List

from fastapi import APIRouter, Path, HTTPException, Depends
from sqlalchemy import select, insert, update, delete
from sqlalchemy.ext.asyncio import AsyncConnection

from .. import dbs
from .db import begin_conn
from .schema import (
    CalibrationImage,
    CalibrationImageAddInput,
    CalibrationImageUpdateInput,
)


router = APIRouter(
    prefix="/calibration-images",
    tags=["calibration-images"],
)

@router.get("/", response_model=List[CalibrationImage])
async def get_calibration_images(
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.CalibrationImage)
    cr = await conn.execute(q)
    return cr.all()


@router.post("/", response_model=CalibrationImage)
async def add_calibration_image(
    img: CalibrationImageAddInput,
    conn: AsyncConnection = Depends(begin_conn),
):
    values = img.dict(by_alias=True, exclude_unset=True)
    qi = insert(dbs.CalibrationImage).values(values)
    cr = await conn.execute(qi)

    # Can't use RETURNING until sqlalchemy 2.0 for SQLITE
    return await get_calibration_image_by_id(cr.inserted_primary_key.id, conn)


@router.get("/{id}", response_model=CalibrationImage)
async def get_calibration_image_by_id(
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):

    q = select(dbs.CalibrationImage).where(dbs.CalibrationImage.id == id_)
    cr = await conn.execute(q)

    r = cr.first()
    if not r:
        raise HTTPException(status_code=404)

    return r


@router.post("/{id}", response_model=CalibrationImage)
async def update_calibration_image_by_id(
    img: CalibrationImageUpdateInput,
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    values = img.dict(by_alias=True, exclude_unset=True)
    q = update(dbs.CalibrationImage).where(
        dbs.CalibrationImage.id == id_
    ).values(
        values
    )
    await conn.execute(q)

    return await get_calibration_image_by_id(id_, conn)


@router.delete("/{id}")
async def delete_calibration_image_by_id(
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = delete(dbs.CalibrationImage).where(dbs.CalibrationImage.id == id_)
    cr = await conn.execute(q)

    if not cr.rowcount:
        raise HTTPException(status_code=404)
