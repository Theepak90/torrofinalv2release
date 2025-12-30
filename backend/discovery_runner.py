import os
import sys
import logging
import json
import pymysql
from datetime import datetime
from typing import List, Dict, Optional


project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

logger = logging.getLogger(__name__)

try:

    from backend.utils.azure_blob_client import AzureBlobClient
    from backend.utils.metadata_extractor import extract_file_metadata, generate_file_hash, generate_schema_hash
    from backend.utils.asset_deduplication import check_asset_exists, should_update_or_insert
    from backend.database import SessionLocal
    from backend.models import Connection, Asset
    from dotenv import load_dotenv
    from pathlib import Path
    import os as os_module
    

    backend_dir = Path(__file__).parent
    env_path = backend_dir / '.env'
    load_dotenv(env_path)
    

    DB_CONFIG = {
        'host': os_module.getenv('DB_HOST', ''),
        'port': int(os_module.getenv('DB_PORT', 3306)),
        'user': os_module.getenv('DB_USER', 'root'),
        'password': os_module.getenv('DB_PASSWORD', ''),
        'database': os_module.getenv('DB_NAME', 'torroforexcel'),
        'charset': 'utf8mb4'
    }
    
    def get_db_connection():
        return pymysql.connect(**DB_CONFIG)
    
    DISCOVERY_AVAILABLE = True
except ImportError as e:
    logger.error('FN:__init__ message:Discovery utilities not available error:{}'.format(str(e)))
    DISCOVERY_AVAILABLE = False


def run_discovery_for_connection(connection_id: int):
    if not DISCOVERY_AVAILABLE:
        logger.error('FN:run_discovery_for_connection message:Discovery utilities not available')
        return
    
    conn = None
    try:

        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
                SELECT id, name, connector_type, connection_type, config, status
                FROM connections
                WHERE id = %s AND connector_type = 'azure_blob' AND status = 'active'
                                            UPDATE assets
                                            SET name = %s,
                                                type = %s,
                                                technical_metadata = %s,
                                                columns = %s,
                                                operational_metadata = %s
                                            WHERE id = %s
                                            INSERT INTO assets (
                                                id, name, type, catalog, connector_id, discovered_at,
                                                technical_metadata, operational_metadata, business_metadata, columns
                                            ) VALUES (
                                                %s, %s, %s, %s, %s, NOW(),
                                                %s, %s, %s, %s
                                            )