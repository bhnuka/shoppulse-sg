from fastapi import FastAPI
from app.api.health import router as health_router

app = FastAPI(title="ShopPulse SG API")

app.include_router(health_router, tags=["health"])

@app.get("/")
def root():
    return {"status": "ok", "service": "shoppulse-sg-api"}
