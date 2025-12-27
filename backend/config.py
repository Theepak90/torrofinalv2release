
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from backend directory
backend_dir = Path(__file__).parent
env_path = backend_dir / '.env'
load_dotenv(env_path)

class Config:
    
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-key-change-in-production")
    FLASK_ENV = os.getenv("FLASK_ENV", "development")
    DEBUG = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    allowed_origins_str = os.getenv("ALLOWED_ORIGINS", "*")
    if allowed_origins_str == "*":
        ALLOWED_ORIGINS = ["*"]
    else:
        ALLOWED_ORIGINS = allowed_origins_str.split(",")

    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE = os.getenv("LOG_FILE", "app.log")

    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "3306")
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "torroforexcel")

    API_VERSION = os.getenv("API_VERSION", "v1")

class DevelopmentConfig(Config):
    
    DEBUG = True
    LOG_LEVEL = "DEBUG"

class ProductionConfig(Config):
    
    DEBUG = False
    LOG_LEVEL = "INFO"

class TestingConfig(Config):
    
    DEBUG = True
    TESTING = True
    DB_NAME = os.getenv("TEST_DB_NAME", "torroforexcel_test")

config = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
    "default": DevelopmentConfig
}

