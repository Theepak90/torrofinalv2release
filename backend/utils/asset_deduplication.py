"""
Deduplication utilities for asset discovery
Adapted from Airflow DAG deduplication logic to work with assets table
"""
import logging
import sys
import os
from typing import Dict, Optional, Tuple
from sqlalchemy.orm import Session

# Import Asset model - handle both relative and absolute imports
# We'll import it lazily when needed to avoid circular imports
Asset = None

def _get_asset_model():
    """Lazy import of Asset model"""
    global Asset
    if Asset is None:
        try:
            # Try relative import first (works when imported as a module)
            from ..models import Asset as AssetModel
            Asset = AssetModel
        except (ImportError, ValueError):
            # Try absolute import
            try:
                backend_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                if backend_path not in sys.path:
                    sys.path.insert(0, backend_path)
                from ..models import Asset as AssetModel
                Asset = AssetModel
            except ImportError:
                # Last resort: import from backend package
                import importlib.util
                models_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'models.py')
                spec = importlib.util.spec_from_file_location("models", models_path)
                models_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(models_module)
                Asset = models_module.Asset
    return Asset

logger = logging.getLogger(__name__)


def normalize_path(path: str) -> str:
    """Normalize storage path for comparison (remove leading/trailing slashes, normalize case)"""
    if not path:
        return ""
    # Remove leading and trailing slashes, but keep internal ones
    normalized = path.strip('/')
    # Normalize to lowercase for case-insensitive comparison
    return normalized.lower()


def check_asset_exists(
    db: Session,
    connector_id: str,
    storage_path: str
):
    """
    Check if an asset already exists based on connector_id and storage path
    
    Args:
        db: Database session
        connector_id: Connector identifier (e.g., 'azure_blob_connection_name')
        storage_path: Full path to the blob/file
    
    Returns:
        Existing Asset if found, None otherwise
    """
    try:
        Asset = _get_asset_model()
        # Normalize the search path
        normalized_search_path = normalize_path(storage_path)
        
        if not normalized_search_path:
            logger.warning(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} message:Empty normalized path')
            return None
        
        # Look for assets with matching connector_id and storage path in technical_metadata
        assets = db.query(Asset).filter(
            Asset.connector_id == connector_id
        ).all()
        
        # Check technical_metadata for matching location/path (normalized comparison)
        for asset in assets:
            tech_meta = asset.technical_metadata or {}
            stored_location = tech_meta.get('location') or tech_meta.get('storage_path') or ""
            normalized_stored = normalize_path(stored_location)
            
            # Exact match after normalization
            if normalized_stored == normalized_search_path:
                logger.debug(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} existing_asset_id:{asset.id} message:Found existing asset')
                return asset
        
        logger.debug(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} message:No existing asset found')
        return None
    except Exception as e:
        logger.error(f'FN:check_asset_exists connector_id:{connector_id} storage_path:{storage_path} error:{str(e)}')
        return None


def get_asset_hashes(asset) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract file hash and schema hash from asset metadata
    
    Returns:
        Tuple of (file_hash, schema_hash)
    """
    tech_meta = asset.technical_metadata or {}
    
    # Try to get hashes from different possible locations
    file_hash = (
        tech_meta.get('file_hash') or
        tech_meta.get('hash', {}).get('value') or
        None
    )
    
    schema_hash = tech_meta.get('schema_hash') or None
    
    return file_hash, schema_hash


def compare_hashes(
    existing_file_hash: Optional[str],
    existing_schema_hash: Optional[str],
    new_file_hash: str,
    new_schema_hash: str
) -> Tuple[bool, bool]:
    """
    Compare existing hashes with new hashes
    
    Returns:
        Tuple of (file_changed, schema_changed)
    """
    file_changed = existing_file_hash != new_file_hash if existing_file_hash else True
    schema_changed = existing_schema_hash != new_schema_hash if existing_schema_hash else True
    
    return file_changed, schema_changed


def should_update_or_insert(
    existing_asset,
    new_file_hash: str,
    new_schema_hash: str
) -> Tuple[bool, bool]:
    """
    Determine if we should insert/update an asset.
    
    Returns:
        Tuple of (should_insert_or_update, schema_changed)
    - Only update full record if schema actually changed
    - For new records, always insert
    - For existing records with only file_hash change, skip update
    """
    if not existing_asset:
        return True, False
    
    existing_file_hash, existing_schema_hash = get_asset_hashes(existing_asset)
    file_changed, schema_changed = compare_hashes(
        existing_file_hash,
        existing_schema_hash,
        new_file_hash,
        new_schema_hash
    )
    
    # Only update full record if schema changed
    if schema_changed:
        logger.info(f'FN:should_update_or_insert schema_changed:True existing_asset_id:{existing_asset.id}')
        return True, True
    
    # File hash changed but schema didn't - skip update
    if file_changed:
        logger.info(f'FN:should_update_or_insert file_changed:True schema_changed:False existing_asset_id:{existing_asset.id}')
        return False, False
    
    # Nothing changed - skip
    logger.info(f'FN:should_update_or_insert file_changed:False schema_changed:False existing_asset_id:{existing_asset.id}')
    return False, False

