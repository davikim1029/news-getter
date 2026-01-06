from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.ext.declarative import declarative_base
from shared_options.log.logger_singleton import getLogger
import os

logger = getLogger()

# ===========================
# Database Setup
# ===========================
DATABASE_URL =  "../option-file-server/database/options.db"

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
    """
    Initialize database tables on startup.
    - Checks if tables exist
    - Creates them if they don't
    - Adds indexes
    """
    from sqlalchemy import inspect
    
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    logger.logMessage(f"[DB] Found existing tables: {existing_tables}")
    
    # Create only our new tables (won't touch existing ones)
    Base.metadata.create_all(bind=engine)
    
    # Verify table creation
    inspector = inspect(engine)
    new_tables = inspector.get_table_names()
    
    if "news_articles" in new_tables:
        logger.logMessage("[DB] ✓ news_articles table ready")
    if "symbol_sentiment" in new_tables:
        logger.logMessage("[DB] ✓ symbol_sentiment table ready")
    if "option_lifetimes" in new_tables:
        logger.logMessage("[DB] ✓ option_lifetimes table exists")
    
    # Add additional indexes if needed
    with engine.connect() as conn:
        # Enable WAL mode for better concurrent access
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        
        # Create compound index for article lookup
        try:
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_symbol_fetched 
                ON news_articles(symbol, fetched_at DESC)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_articles_url 
                ON news_articles(url)
            """)
            logger.logMessage("[DB] ✓ Additional indexes created")
        except Exception as e:
            logger.logMessage(f"[DB] Index creation note: {e}")
        
        conn.commit()
    
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
