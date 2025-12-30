import logging
import sys
import os
from typing import Dict, Optional, Tuple
from sqlalchemy.orm import Session



Asset = None

def _get_asset_model():
    global Asset
    if Asset is None:
        try:

            import importlib.util
            backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            models_path = os.path.join(backend_path, 'models.py')
            if os.path.exists(models_path):
                spec = importlib.util.spec_from_file_location("models", models_path)
                models_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(models_module)
                Asset = models_module.Asset
            else:

                from models import Asset as AssetModel
                Asset = AssetModel
        except (ImportError, ValueError, Exception) as e:

            try:
                backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if backend_path not in sys.path:
                    sys.path.insert(0, backend_path)
                from models import Asset as AssetModel
                Asset = AssetModel
            except ImportError:
                logger.error(f'FN:_get_asset_model error:Could not import Asset model error:{str(e)}')
                raise
    return Asset

logger = logging.getLogger(__name__)


def normalize_path(path: str) -> str:
    if not path:
        return ""

    normalized = path.strip('/')

    return normalized.lower()


def check_asset_exists(
    db: Session,
    connector_id: str,
    storage_path: str
):
    try:
        Asset = _get_asset_model()

        normalized_search_path = normalize_path(storage_path)
        
        if not normalized_search_path:
            logger.warning(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} message:Empty normalized path')
            return None
        



        from sqlalchemy import text
        

        query = text("""
            SELECT id FROM assets 
            WHERE connector_id = :connector_id 
            AND (
                JSON_UNQUOTE(JSON_EXTRACT(technical_metadata, '$.location')) = :path
                OR JSON_UNQUOTE(JSON_EXTRACT(technical_metadata, '$.storage_path')) = :path
            )
            LIMIT 1
    Extract file hash and schema hash from asset metadata
    
    Returns:
        Tuple of (file_hash, schema_hash)
    Compare existing hashes with new hashes
    
    Returns:
        Tuple of (file_changed, schema_changed)
    Determine if we should insert/update an asset.
    
    Returns:
        Tuple of (should_insert_or_update, schema_changed)
    - Only update full record if schema actually changed
    - For new records, always insert
    - For existing records with only file_hash change, skip update