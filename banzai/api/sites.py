from typing import List

from fastapi import APIRouter, Path, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import select, insert, update, delete
from sqlalchemy.ext.asyncio import AsyncConnection

from .. import dbs
from .db import begin_conn
from .instruments import Instrument


router = APIRouter(prefix="/sites", tags=["sites"])


class Site(BaseModel):
  id_: str = Field(alias="id")
  timezone: int
  latitude: float
  longitude: float
  elevation: float


class SiteUpdateInput(BaseModel):
  timezone: int
  latitude: float
  longitude: float
  elevation: float


@router.get("/", response_model=List[Site])
async def get_sites(
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.Site)
    r = await conn.execute(q)
    return r.all()


@router.post("/", response_model=Site)
async def add_site(
    site: Site,
    conn: AsyncConnection = Depends(begin_conn),
):
    q = insert(dbs.Site).values(site.dict(by_alias=True))
    await conn.execute(q)
    return site


@router.get("/{id}", response_model=Site)
async def get_site_by_id(
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.Site).where(dbs.Site.id == id_)
    cr = await conn.execute(q)

    r = cr.first()

    if r is None:
        raise HTTPException(status_code=404)

    return r

@router.post("/{id}", response_model=Site)
async def update_site_by_id(
    site: SiteUpdateInput,
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    values = site.dict(by_alias=True)
    q = update(dbs.Site).where(dbs.Site.id == id_).values(
      values
    )
    await conn.execute(q)

    return {"id": id_, **values}


@router.delete("/{id}")
async def delete_site_by_id(
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = delete(dbs.Site).where(dbs.Site.id == id_)
    cr = await conn.execute(q)

    if not cr.rowcount:
        raise HTTPException(status_code=404)


@router.get("/{id}/instruments", response_model=List[Instrument])
async def get_site_instruments(
    id_: str = Path(alias="id"),
    conn: AsyncConnection = Depends(begin_conn),
):
    q = select(dbs.Instrument).where(dbs.Instrument.site == id_)
    cr = await conn.execute(q)

    r = cr.all()

    return r
