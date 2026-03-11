from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from shared_options.log.logger_singleton import getLogger

logger = getLogger()

# ===========================
# Database Setup
# ===========================
DATABASE_URL = "sqlite:///../option-file-server/database/options.db"

# SQLite-specific engine configuration
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # Needed for SQLite
    pool_pre_ping=True
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ===========================

def init_database():
    """Initialize news-getter schema via versioned migrations."""
    from database.migrations import run_migrations
    db_path = DATABASE_URL.replace("sqlite:///", "")
    run_migrations(db_path)
    logger.logMessage("[DB] Database initialization complete")


# ===========================
# Dependency Injection
# ===========================
def get_db():
    """Database session dependency"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
