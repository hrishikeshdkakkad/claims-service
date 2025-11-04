"""
Database configuration and session management.
Supports both SQLite (for development) and PostgreSQL (for production).
"""
import os
from typing import Generator
from sqlmodel import SQLModel, Session, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool


# Database URL configuration
def get_database_url() -> str:
    """
    Get database URL from environment or use default.
    Supports both SQLite and PostgreSQL.
    """
    # Check for PostgreSQL URL (production/docker)
    postgres_url = os.getenv("DATABASE_URL")
    if postgres_url:
        # Handle both postgres:// and postgresql:// prefixes
        if postgres_url.startswith("postgres://"):
            postgres_url = postgres_url.replace("postgres://", "postgresql://", 1)
        return postgres_url

    # Default to SQLite for local development
    sqlite_path = os.getenv("SQLITE_PATH", "claim_process.db")
    return f"sqlite:///{sqlite_path}"


# Create engine based on database type
def create_db_engine() -> Engine:
    """
    Create database engine with appropriate configuration.
    """
    database_url = get_database_url()

    if database_url.startswith("sqlite"):
        # SQLite configuration for development
        # check_same_thread=False allows multiple threads
        # poolclass=StaticPool maintains a single connection
        engine = create_engine(
            database_url,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
            echo=os.getenv("SQL_ECHO", "false").lower() == "true"
        )
    else:
        # PostgreSQL configuration for production
        engine = create_engine(
            database_url,
            pool_size=int(os.getenv("DB_POOL_SIZE", "20")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "40")),
            pool_pre_ping=True,  # Verify connections before using
            echo=os.getenv("SQL_ECHO", "false").lower() == "true"
        )

    return engine


# Global engine instance
engine = create_db_engine()


def create_db_and_tables():
    """
    Create database tables if they don't exist.
    Note: In production, use Alembic migrations instead.
    """
    SQLModel.metadata.create_all(engine)


def get_session() -> Generator[Session, None, None]:
    """
    Dependency injection for database sessions.
    Ensures proper session lifecycle management.
    """
    with Session(engine) as session:
        yield session


def init_db():
    """
    Initialize database (create tables if not using migrations).
    This is mainly for development/testing.
    """
    # Import models to ensure they're registered
    from claim_process.models import Claim

    # Create tables
    create_db_and_tables()

    print(f"Database initialized with URL: {get_database_url()}")