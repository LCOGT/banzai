from typing import List

from fastapi import APIRouter, Path, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select, insert, update, delete

from .. import dbs
from .db import database


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
async def get_sites():
    q = select(dbs.Site)
    return await database.fetch_all(q)


@router.post("/", response_model=Site)
async def create_site(site: Site):
    q = insert(dbs.Site).values(site.dict(by_alias=True))
    await database.execute(q)
    return site


@router.get("/{id}", response_model=Site)
async def get_site_by_id(id_: str = Path(alias="id")):
    q = select(dbs.Site).where(dbs.Site.id == id_)
    r = await database.fetch_one(q)
    if not r:
        raise HTTPException(status_code=404)
    return r


@router.post("/{id}", response_model=Site)
async def update_site_by_id(site: SiteUpdateInput, id_: str = Path(alias="id")):
    q = update(dbs.Site).where(dbs.Site.id == id_).values(site.dict())
    await database.execute(q)
    return {"id": id_, **site.dict()}


@router.delete("/{id}")
async def delete_site_by_id(id_: str = Path(alias="id")):
    q = delete(dbs.Site).where(dbs.Site.id == id_)
    r = await database.execute(q)
    if not r:
        raise HTTPException(status_code=404)
