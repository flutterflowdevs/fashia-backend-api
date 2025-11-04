from fastapi import APIRouter
from app.db.session import get_db 
from app.services.sql_lite_service import get_entity_count

router = APIRouter()

@router.get("/")
async def root():
    return {"status": "ok", "message": "Fashia Backend API"}

@router.get("/api/hello")
async def hello():
    return {"message": "Hello from Fashia API!"}

@router.get("/api/health")
async def health():
    return {"status": "ok", "service": "Fashia Backend API"}


@router.get("/entities/count")
def read_entity_count():
    count = get_entity_count()
    return {"count": count}

@router.get("/hello/dev")
def read_entity_count():
    return "Hello from Fashia Development!"