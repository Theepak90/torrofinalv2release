import os
import sys
from typing import Dict, Optional
from dotenv import load_dotenv

# Load .env from airflow directory
airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(airflow_dir, '.env')
load_dotenv(env_path)

# Add utils directory to path for imports
utils_dir = os.path.join(airflow_dir, 'utils')
if utils_dir not in sys.path:
    sys.path.insert(0, utils_dir)

try:
    from utils.storage_path_parser import parse_storage_path
    PARSER_AVAILABLE = True
except ImportError:
    PARSER_AVAILABLE = False
    import logging
    logger = logging.getLogger(__name__)
    logger.warning('FN:azure_config storage_path_parser not available, using legacy path handling')

# Azure connections are now stored in the database (created via frontend)
# This is kept as a fallback only if database is unavailable
AZURE_STORAGE_ACCOUNTS = [
    {
        "name": os.getenv("AZURE_STORAGE_ACCOUNT_NAME", ""),
        "connection_string": os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
        "containers": [c.strip() for c in os.getenv("AZURE_CONTAINERS", "").split(",") if c.strip()],
        "folders": [f.strip() for f in os.getenv("AZURE_FOLDERS", "").split(",") if f.strip()],
        "environment": os.getenv("AZURE_ENVIRONMENT", "prod"),
        "env_type": os.getenv("AZURE_ENV_TYPE", "production"),
        "data_source_type": os.getenv("AZURE_DATA_SOURCE_TYPE", "credit_card"),
        "file_extensions": None,  # None = discover all files
    }
]
# Note: This fallback will only be used if get_azure_connections_from_db() fails
# Primary source of connections is the database (created via frontend)

DISCOVERY_CONFIG = {
    "schedule_interval": "*/1 * * * *",
    "notification_recipients": [email.strip() for email in os.getenv("NOTIFICATION_EMAILS", "").split(",") if email.strip()],
    "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "smtp_port": int(os.getenv("SMTP_PORT", "587")),
    "smtp_user": os.getenv("SMTP_USER", ""),
    "smtp_password": os.getenv("SMTP_PASSWORD", ""),
}

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "torroforexcel"),  # Use main database
}

# Azure AI Language (DLP) Configuration
AZURE_AI_LANGUAGE_CONFIG = {
    "endpoint": os.getenv("AZURE_AI_LANGUAGE_ENDPOINT", ""),
    "key": os.getenv("AZURE_AI_LANGUAGE_KEY", ""),
    "enabled": bool(os.getenv("AZURE_AI_LANGUAGE_ENDPOINT") and os.getenv("AZURE_AI_LANGUAGE_KEY"))
}

def get_storage_location_json(account_name: str, container: str, blob_path: str, 
                              connection_string: Optional[str] = None) -> Dict:
    """
    Get storage location JSON with support for ABFS URLs and standard blob paths.
    
    Args:
        account_name: Storage account name (used as fallback for simple paths)
        container: Container name (used as fallback for simple paths)
        blob_path: Blob path or full ABFS URL (e.g., abfs://container@account.dfs.core.windows.net/path)
        connection_string: Optional connection string (for backward compatibility)
    
    Returns:
        Storage location dictionary with parsed information
    """
    # Try to parse the path using the parser system
    if PARSER_AVAILABLE:
        try:
            parsed = parse_storage_path(blob_path, account_name, container)
            
            # If parsing succeeded, use the parsed result
            # Merge with connection info if provided
            result = {
                "type": parsed.get("type", "azure_blob"),
                "path": parsed.get("path", blob_path),
                "connection": parsed.get("connection", {}),
                "container": parsed.get("container_info", {}),
                "metadata": parsed.get("metadata", {})
            }
            
            # Add connection_string if provided (for backward compatibility)
            if connection_string:
                result["connection"]["connection_string"] = connection_string
            elif not result["connection"].get("connection_string"):
                # Fallback to environment variable
                result["connection"]["connection_string"] = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
            
            # Ensure account_name is set
            if not result["connection"].get("account_name"):
                result["connection"]["account_name"] = parsed.get("account_name") or account_name
            
            # Add protocol and full_url if available
            if parsed.get("protocol"):
                result["metadata"]["protocol"] = parsed.get("protocol")
            if parsed.get("full_url"):
                result["metadata"]["full_url"] = parsed.get("full_url")
            
            return result
        except (ValueError, Exception) as e:
            # If parsing fails, fall back to legacy behavior
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f'FN:get_storage_location_json path:{blob_path} parser_failed:{str(e)} using_legacy')
    
    # Legacy behavior (backward compatibility)
    return {
        "type": "azure_blob",
        "path": blob_path,
        "connection": {
            "method": "connection_string",
            "connection_string": connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING", ""),
            "account_name": account_name
        },
        "container": {
            "name": container,
            "type": "blob_container"
        },
        "metadata": {}
    }


def get_azure_connections_from_db():
    """
    Get all active Azure Blob connections from the main application database.
    This function is used by the Airflow DAG to automatically discover new connections.
    
    Returns:
        List of connection dictionaries with: id, name, connector_type, connection_type, config, status
    """
    import pymysql
    import json
    import logging
    
    logger = logging.getLogger(__name__)
    
    db_conn = None
    try:
        db_conn = pymysql.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            cursorclass=pymysql.cursors.DictCursor,
            charset='utf8mb4'
        )
        with db_conn.cursor() as cursor:
            sql = """
                SELECT id, name, connector_type, connection_type, config, status
                FROM connections
                WHERE connector_type = 'azure_blob' AND status = 'active'
            """
            cursor.execute(sql)
            connections = cursor.fetchall()
            
            # Parse config JSON if it's a string
            for conn_item in connections:
                if isinstance(conn_item.get("config"), str):
                    try:
                        conn_item["config"] = json.loads(conn_item["config"])
                    except json.JSONDecodeError:
                        logger.warning('FN:get_azure_connections_from_db connection_id:{} message:Invalid JSON config'.format(conn_item.get("id")))
                        conn_item["config"] = {}
            
            logger.info('FN:get_azure_connections_from_db found_connections:{}'.format(len(connections)))
            return connections
    except Exception as e:
        logger.error('FN:get_azure_connections_from_db error:{}'.format(str(e)))
        raise
    finally:
        if db_conn:
            db_conn.close()
