import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from .routers import routers


app = FastAPI(debug=True, reload=True)

static_path = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_path), name="static")

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins = origins 
)

PREFIX = "/api/v1"

for router in routers:
    app.include_router(router, prefix=PREFIX)

