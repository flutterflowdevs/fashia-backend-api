import os
from pydantic_settings import BaseSettings # type: ignore
from functools import lru_cache

class Settings(BaseSettings):
    # Application Settings
    app_name: str = "Fashia Backend API"
    app_version: str = "1.0.0"
    debug: bool = True
    
    # Database Settings (Supabase)
    database_url: str = os.getenv("DATABASE_URL", "postgresql://user:password@localhost:5432/fashia")
    
    # Supabase Settings
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_KEY", "")
    
    class Config:
        env_file = ".env"
        case_sensitive = False

@lru_cache()
def get_settings() -> Settings:
    return Settings()

settings = get_settings()
