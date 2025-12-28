import os
import sys
from typing import Dict, Optional

# Import centralized config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import config

# Add utils directory to path for imports
airflow_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
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

# Use config values (backward compatibility - expose as module-level variables)
AZURE_STORAGE_ACCOUNTS = config.AZURE_STORAGE_ACCOUNTS
DISCOVERY_CONFIG = config.DISCOVERY_CONFIG
DB_CONFIG = config.DB_CONFIG
AZURE_AI_LANGUAGE_CONFIG = config.AZURE_AI_LANGUAGE_CONFIG

# Note: Azure connections are now stored in the database (created via frontend)
# AZURE_STORAGE_ACCOUNTS is kept as a fallback only if database is unavailable
# Primary source of connections is the database (created via frontend)

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
                # Fallback to config
                result["connection"]["connection_string"] = config.AZURE_STORAGE_CONNECTION_STRING
            
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
            "connection_string": connection_string or config.AZURE_STORAGE_CONNECTION_STRING,
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
