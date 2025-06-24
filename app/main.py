import os
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from .routers import routers


app = FastAPI(debug=True, reload=True)

static_path = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_path), name="static")

origins = ["https://crm-alpha.maxapex.net", "http://127.0.0.1:8000", "https://34.142.255.255"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

PREFIX = "/api/v1"

for router in routers:
    app.include_router(router, prefix=PREFIX)

