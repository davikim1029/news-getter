from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker 
from sqlalchemy.ext.declarative import declarative_base
from shared_options.log.logger_singleton import getLogger
import os

logger = getLogger()

# ===========================
# Database Setup
# ===========================
DATABASE_URL =  "sqlite:///../option-file-server/database/options.db"

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
    - Checks if database file exists, creates if needed
    - Checks if tables exist
    - Creates only missing tables (won't touch existing ones)
    - Adds indexes
    """
    import os
    from sqlalchemy import inspect, text
    
    # Parse database path from URL
    db_path = DATABASE_URL.replace("sqlite:///", "")
    db_dir = os.path.dirname(db_path)
    
    # Create database directory if it doesn't exist
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.logMessage(f"[DB] Created directory: {db_dir}")
    
    # Check if database file exists
    if not os.path.exists(db_path):
        logger.logMessage(f"[DB] Database file not found, will create: {db_path}")
    else:
        logger.logMessage(f"[DB] Found existing database: {db_path}")
    
    # Check existing tables
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    logger.logMessage(f"[DB] Existing tables: {existing_tables}")
    
    # Create only our new tables (Base.metadata.create_all only creates missing tables)
    # This will NOT modify or recreate existing tables
    try:
        # This creates news_articles and symbol_sentiment tables if they don't exist
        Base.metadata.create_all(bind=engine)
        logger.logMessage("[DB] Table creation completed")
    except Exception as e:
        logger.logMessage(f"[DB] Error during table creation: {e}")
        raise
    
    # Verify table creation
    inspector = inspect(engine)
    final_tables = inspector.get_table_names()
    
    if "news_articles" in final_tables:
        logger.logMessage("[DB] ✓ news_articles table ready")
        # Show schema for verification
        columns = [col['name'] for col in inspector.get_columns('news_articles')]
        logger.logMessage(f"[DB]   Columns: {', '.join(columns)}")
    else:
        logger.logMessage("[DB] ✗ news_articles table MISSING!")
    
    if "symbol_sentiment" in final_tables:
        logger.logMessage("[DB] ✓ symbol_sentiment table ready")
        columns = [col['name'] for col in inspector.get_columns('symbol_sentiment')]
        logger.logMessage(f"[DB]   Columns: {', '.join(columns)}")
    else:
        logger.logMessage("[DB] ✗ symbol_sentiment table MISSING!")
    
    if "option_lifetimes" in final_tables:
        logger.logMessage("[DB] ✓ option_lifetimes table exists (existing)")
    else:
        logger.logMessage("[DB] ⚠ option_lifetimes table not found - make sure database path is correct")
    
    # Add additional indexes and optimize database
    with engine.connect() as conn:
        try:
            # Enable WAL mode for better concurrent access
            conn.execute(text("PRAGMA journal_mode=WAL"))
            conn.execute(text("PRAGMA synchronous=NORMAL"))
            logger.logMessage("[DB] ✓ Enabled WAL mode for better concurrency")
            
            # Additional indexes (if not already created by SQLAlchemy)
            # These are idempotent - won't error if they exist
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_articles_url 
                ON news_articles(url)
            """))
            
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_sentiment_updated 
                ON symbol_sentiment(last_updated DESC)
            """))
            
            logger.logMessage("[DB] ✓ Additional indexes verified")
            
            conn.commit()
            
        except Exception as e:
            logger.logMessage(f"[DB] Note during optimization: {e}")
    
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
