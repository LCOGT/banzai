from fastapi import FastAPI

from . import sites, instruments, calibration_images
from .db import init_models, engine


app = FastAPI(
    title="banzai-api",
    version="0.1.0",
    docs_url="/",
    redoc_url="/redoc",
    debug=True,
)
app.include_router(sites.router)
app.include_router(instruments.router)
app.include_router(calibration_images.router)


@app.on_event("startup")
async def startup():
    await init_models()


@app.on_event("shutdown")
async def shutdown():
    await engine.dispose()
