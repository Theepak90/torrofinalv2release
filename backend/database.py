from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import QueuePool
import os
from urllib.parse import quote_plus

# Import config to ensure .env is loaded and use config values
try:
    from config import config
    # Get the active config (development, production, or default)
    env = os.getenv("FLASK_ENV", "development")
    active_config = config.get(env, config["default"])
    
    DB_HOST = active_config.DB_HOST
    DB_PORT = active_config.DB_PORT
    DB_USER = active_config.DB_USER
    DB_PASSWORD = active_config.DB_PASSWORD
    DB_NAME = active_config.DB_NAME
    DB_POOL_SIZE = active_config.DB_POOL_SIZE
    DB_MAX_OVERFLOW = active_config.DB_MAX_OVERFLOW
    DB_POOL_RECYCLE = active_config.DB_POOL_RECYCLE
except ImportError:
    # Fallback if config not available (shouldn't happen in normal operation)
    from dotenv import load_dotenv
    from pathlib import Path
    
    backend_dir = Path(__file__).parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    
    # DB_HOST should be set in .env file (no default to force explicit configuration)
    DB_HOST = os.getenv("DB_HOST", "")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "torroforexcel")
    DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "75"))
    DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "75"))
    DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "3600"))
    
    # Validate required database configuration
    if not DB_HOST:
        raise ValueError("DB_HOST must be set in backend/.env file. Cannot use default 'localhost'.")

DB_DRIVER = os.getenv("DB_DRIVER", "pymysql")

if DB_PASSWORD:

    encoded_password = quote_plus(DB_PASSWORD)
    DATABASE_URL = f"mysql+{DB_DRIVER}://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
else:
    DATABASE_URL = f"mysql+{DB_DRIVER}://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

if os.getenv("DATABASE_URL"):
    DATABASE_URL = os.getenv("DATABASE_URL")

# Connection pool configuration for production (20-30 concurrent users)
# pool_size: Base connections always available (75 = ~3 per user for 25 users)
# max_overflow: Additional connections during peak load (75 = total max 150)
# This handles 20-30 concurrent users with 2-3 requests per user
# Values are now loaded from config above
POOL_SIZE = DB_POOL_SIZE
MAX_OVERFLOW = DB_MAX_OVERFLOW
POOL_RECYCLE = DB_POOL_RECYCLE

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=True,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true"
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

