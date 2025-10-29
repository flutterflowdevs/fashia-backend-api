from fastapi import APIRouter

router = APIRouter()

@router.get("/api/hello")
async def hello():
    return {"message": "Hello from Fashia API!"}

@router.get("/api/health")
async def health():
    return {"status": "ok", "service": "Fashia Backend API"}
