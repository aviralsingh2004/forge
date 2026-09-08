from fastapi import FastAPI

from forge.api.health import router as health_router
from forge.api.jobs import router as jobs_router

app = FastAPI(title="Forge")
app.include_router(health_router)
app.include_router(jobs_router)
