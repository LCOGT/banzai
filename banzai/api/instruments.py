from typing import List

from fastapi import APIRouter, Path, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select, insert, update, delete

from .. import dbs
from .db import database


router = APIRouter(prefix="/instruments", tags=["instruments"])


class Instrument(BaseModel):
  id_: int = Field(alias="id")
  site: str
  camera: str
  type_: str = Field(alias="type")
  name: str

class InstrumentCreateInput(BaseModel):
  site: str
  camera: str
  type_: str = Field(alias="type")
  name: str


class InstrumentUpdateInput(BaseModel):
  site: str
  camera: str
  type_: str = Field(alias="type")
  name: str


@router.get("/", response_model=List[Instrument])
async def get_instruments():
    q = select(dbs.Instrument)
    return await database.fetch_all(q)


@router.post("/", response_model=InstrumentCreateInput)
async def create_instrument(inst: InstrumentCreateInput):
    q = insert(dbs.Instrument).values(inst.dict(by_alias=True))
    await database.execute(q)
    return inst


@router.get("/{id}", response_model=Instrument)
async def get_instrument_by_id(id_: int = Path(alias="id")):
    q = select(dbs.Instrument).where(dbs.Instrument.id == id_)
    r = await database.fetch_one(q)
    if not r:
        raise HTTPException(status_code=404)
    return r


@router.post("/{id}", response_model=Instrument)
async def update_instrument_by_id(inst: InstrumentUpdateInput, id_: int = Path(alias="id")):
    q = update(dbs.Instrument).where(dbs.Instrument.id == id_).values(inst.dict(by_alias=True))
    await database.execute(q)
    return {"id": id_, **inst.dict(by_alias=True)}


@router.delete("/{id}")
async def delete_instrument_by_id(id_: int = Path(alias="id")):
    q = delete(dbs.Instrument).where(dbs.Instrument.id == id_)
    r = await database.execute(q)
    if not r:
        raise HTTPException(status_code=404)
