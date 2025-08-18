from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core import settings

# SQLite specific configuration
if "sqlite" in settings.SQLALCHEMY_DATABASE_URI:
    engine = create_engine(
        settings.SQLALCHEMY_DATABASE_URI,
        connect_args={"check_same_thread": False},  # Needed for SQLite
        poolclass=StaticPool,  # Better for SQLite
        pool_pre_ping=True
    )
else:
    engine = create_engine(
        settings.SQLALCHEMY_DATABASE_URI, 
        pool_pre_ping=True
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
