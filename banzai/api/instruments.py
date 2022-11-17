from typing import List

from fastapi import APIRouter, Path, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, insert, update, delete
from sqlalchemy.ext.asyncio import AsyncConnection

from .. import dbs
from .db import begin_conn
from .calibration_images import CalibrationImage


router = APIRouter(prefix="/instruments", tags=["instruments"])


class InstrumentAddInput(BaseModel):
    site: str
    camera: str
    type_: str = Field(alias="type")
    name: str

class Instrument(InstrumentAddInput):
    id_: int = Field(alias="id")

class InstrumentUpdateInput(InstrumentAddInput):
    pass


@router.get("/", response_model=List[Instrument])
async def get_instruments(
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.Instrument)
    cr = await conn.execute(q)
    return cr.all()


@router.post("/", response_model=Instrument)
async def add_instrument(
    inst: InstrumentAddInput,
    conn: AsyncConnection = Depends(begin_conn),
):
    values = inst.dict(by_alias=True)
    q = insert(dbs.Instrument).values(values)
    cr = await conn.execute(q)

    return {"id": cr.inserted_primary_key.id, **values}


@router.get("/{id}", response_model=Instrument)
async def get_instrument_by_id(
    id_: int = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.Instrument).where(dbs.Instrument.id == id_)
    c = await conn.execute(q)

    r = c.first()
    if r is None:
        raise HTTPException(status_code=404)

    return r


@router.post("/{id}", response_model=Instrument)
async def update_instrument_by_id(
    inst: InstrumentUpdateInput,
    id_: int = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    values = inst.dict(by_alias=True)
    q = update(dbs.Instrument).where(dbs.Instrument.id == id_).values(
      values
    )
    await conn.execute(q)

    return {"id": id_, **values}


@router.delete("/{id}")
async def delete_instrument_by_id(
    id_: int = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = delete(dbs.Instrument).where(dbs.Instrument.id == id_)
    cr = await conn.execute(q)

    if not cr.rowcount:
        raise HTTPException(status_code=404)


@router.get("/{id}/calibration-images", response_model=List[CalibrationImage])
async def get_instrument_calibration_images(
    id_: int = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.CalibrationImage).where(dbs.CalibrationImage.instrument_id == id_)
    cr = await conn.execute(q)

    r = cr.all()

    return r
