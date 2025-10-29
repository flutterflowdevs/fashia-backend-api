from fastapi import Depends
from sqlalchemy.orm import Session
from app.db.session import get_db

def get_db_session() -> Session:
    db = get_db()
    try:
        yield db
    finally:
        db.close()