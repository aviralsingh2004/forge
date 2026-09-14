from fastapi import FastAPI

from forge.api.attempts import router as attempts_router
from forge.api.health import router as health_router
from forge.api.jobs import router as jobs_router
from forge.api.workers import router as workers_router

app = FastAPI(title="Forge")
app.include_router(attempts_router)
app.include_router(health_router)
app.include_router(jobs_router)
app.include_router(workers_router)
