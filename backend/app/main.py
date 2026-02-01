from fastapi import FastAPI
from app.api.health import router as health_router
from app.api.registry import router as registry_router
from app.api.chat import router as chat_router

app = FastAPI(title="ShopPulse SG API")

app.include_router(health_router, tags=["health"])
app.include_router(registry_router, tags=["registry"])
app.include_router(chat_router, tags=["chat"])

@app.get("/")
def root():
    return {"status": "ok", "service": "shoppulse-sg-api"}
