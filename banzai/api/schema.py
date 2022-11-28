from typing import Optional
from datetime import datetime, timezone

from pydantic import BaseModel, Field, constr, StrictInt, validator


def encode_datetime(v: datetime) -> str:
  return "%sZ" % v.astimezone(timezone.utc).replace(tzinfo=None).isoformat(
      timespec="milliseconds"
  )


class _BaseModel(BaseModel):

    class Config:
        json_encoders = {
            datetime: encode_datetime,
        }


class SiteUpdateInput(_BaseModel):
  timezone: int
  latitude: float
  longitude: float
  elevation: float


class Site(SiteUpdateInput):
  id_: str = Field(alias="id")


class InstrumentAddInput(_BaseModel):
    site: str
    camera: str
    type_: str = Field(alias="type")
    name: str


class Instrument(InstrumentAddInput):
    id_: int = Field(alias="id")


class InstrumentUpdateInput(InstrumentAddInput):
    pass


class CalibrationImageAddInput(_BaseModel):
    type_: str = Field(alias="type")
    filename: constr(max_length=100) # pyright: ignore
    filepath: constr(max_length=150) # pyright: ignore
    frameid: Optional[int]
    dateobs: datetime
    datecreated: datetime
    instrument_id: StrictInt
    is_master: bool
    is_bad: bool
    good_until: Optional[datetime]
    good_after: Optional[datetime]
    attributes: Optional[dict] = {}

    @validator("dateobs", "datecreated", "good_until", "good_after")
    def to_utc_datetime_without_tz(cls, v):
        if isinstance(v, datetime):
            return v.astimezone(timezone.utc).replace(tzinfo=None)
        return v


class CalibrationImage(CalibrationImageAddInput):
    id_: int = Field(alias="id")

    @validator("dateobs", "datecreated", "good_until", "good_after")
    def to_utc_datetime_with_tz(cls, v):
        if isinstance(v, datetime):
            return v.astimezone(timezone.utc)
        return v


class CalibrationImageUpdateInput(CalibrationImageAddInput):
    pass


class ProcessedImage(_BaseModel):
    id_: int = Field(alias="id")
    filename: constr(max_length=100) # pyright: ignore
    frameid: Optional[int] = None
    checksum: constr(max_length=32) # pyright: ignore
    success: bool = False
    tries: int = 0
