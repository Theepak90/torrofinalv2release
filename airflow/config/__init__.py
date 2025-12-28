# Config package - centralized configuration for Airflow
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env from airflow directory
airflow_dir = Path(__file__).parent.parent
env_path = airflow_dir / '.env'
load_dotenv(env_path)

class AirflowConfig:
    """Centralized configuration for Airflow components"""
    
    # Database Configuration
    MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_USER = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "torroforexcel")
    
    # Database Connection Pool Configuration (for retry logic)
    DB_RETRY_MAX_ATTEMPTS = int(os.getenv("DB_RETRY_MAX_ATTEMPTS", "20"))
    
    # Notification Configuration
    NOTIFICATION_EMAILS = [e.strip() for e in os.getenv("NOTIFICATION_EMAILS", "").split(",") if e.strip()]
    SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SMTP_USER = os.getenv("SMTP_USER", "")
    SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
    
    # Airflow Configuration
    AIRFLOW_DAG_SCHEDULE = os.getenv("AIRFLOW_DAG_SCHEDULE", "0 * * * *")
    FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5162")
    
    # Azure AI Language (PII Detection) Configuration
    AZURE_AI_LANGUAGE_ENDPOINT = os.getenv("AZURE_AI_LANGUAGE_ENDPOINT", "")
    AZURE_AI_LANGUAGE_KEY = os.getenv("AZURE_AI_LANGUAGE_KEY", "")
    
    # Azure Storage Configuration (fallback - primary source is database)
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "")
    AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    AZURE_CONTAINERS = [c.strip() for c in os.getenv("AZURE_CONTAINERS", "").split(",") if c.strip()]
    AZURE_FOLDERS = [f.strip() for f in os.getenv("AZURE_FOLDERS", "").split(",") if f.strip()]
    AZURE_ENVIRONMENT = os.getenv("AZURE_ENVIRONMENT", "prod")
    AZURE_ENV_TYPE = os.getenv("AZURE_ENV_TYPE", "production")
    AZURE_DATA_SOURCE_TYPE = os.getenv("AZURE_DATA_SOURCE_TYPE", "credit_card")
    
    @property
    def DB_CONFIG(self):
        """Database configuration dictionary"""
        return {
            "host": self.MYSQL_HOST,
            "port": self.MYSQL_PORT,
            "user": self.MYSQL_USER,
            "password": self.MYSQL_PASSWORD,
            "database": self.MYSQL_DATABASE,
            "charset": "utf8mb4"
        }
    
    @property
    def DISCOVERY_CONFIG(self):
        """Discovery configuration dictionary"""
        return {
            "schedule_interval": "*/1 * * * *",
            "notification_recipients": self.NOTIFICATION_EMAILS,
            "smtp_server": self.SMTP_SERVER,
            "smtp_port": self.SMTP_PORT,
            "smtp_user": self.SMTP_USER,
            "smtp_password": self.SMTP_PASSWORD,
        }
    
    @property
    def AZURE_AI_LANGUAGE_CONFIG(self):
        """Azure AI Language (DLP) configuration dictionary"""
        return {
            "endpoint": self.AZURE_AI_LANGUAGE_ENDPOINT,
            "key": self.AZURE_AI_LANGUAGE_KEY,
            "enabled": bool(self.AZURE_AI_LANGUAGE_ENDPOINT and self.AZURE_AI_LANGUAGE_KEY)
        }
    
    @property
    def AZURE_STORAGE_ACCOUNTS(self):
        """Azure storage accounts configuration (fallback only)"""
        return [
            {
                "name": self.AZURE_STORAGE_ACCOUNT_NAME,
                "connection_string": self.AZURE_STORAGE_CONNECTION_STRING,
                "containers": self.AZURE_CONTAINERS,
                "folders": self.AZURE_FOLDERS,
                "environment": self.AZURE_ENVIRONMENT,
                "env_type": self.AZURE_ENV_TYPE,
                "data_source_type": self.AZURE_DATA_SOURCE_TYPE,
                "file_extensions": None,  # None = discover all files
            }
        ]

# Create config instance
config = AirflowConfig()
