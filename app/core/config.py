import secrets
import os
from pathlib import Path

from pydantic import BaseSettings


class Settings(BaseSettings):
    API_V1_STR: str = "/api/v1"
    PROJECT_NAME: str = "FastAPI"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    
    # SQLite configuration
    DATABASE_URL: str = "sqlite:///./app.db"
    
    # For Alembic compatibility
    SQLALCHEMY_DATABASE_URI: str = "sqlite:///./app.db"
    
    # SQLite specific settings
    SQLITE_DB_PATH: str = "./app.db"

    class Config:
        env_file = ".env"


settings = Settings()
