"""
Storage Path Parser Plugin Architecture

Supports parsing different storage URL formats:
- ABFS/ABFSS: Azure Data Lake Storage Gen2 (abfs://container@account.dfs.core.windows.net/path)
- Azure Blob: Standard blob paths (container/path/file.csv)
- S3: AWS S3 (s3://bucket/path/file.csv) - Future
- GCS: Google Cloud Storage (gs://bucket/path/file.csv) - Future
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional
import re
import logging

logger = logging.getLogger(__name__)


class StoragePathParser(ABC):
    """Base class for storage path parsers"""
    
    @abstractmethod
    def can_parse(self, path: str) -> bool:
        """Check if this parser can handle this path format"""
        pass
    
    @abstractmethod
    def parse(self, path: str) -> Dict:
        """
        Parse path into standardized format
        
        Returns:
            Dict with keys:
                - type: Storage type (azure_blob, azure_datalake, s3, gcs)
                - account_name: Storage account name (for Azure)
                - container: Container/bucket name
                - path: File path within container
                - protocol: Original protocol (abfs, abfss, s3, gs, etc.)
                - full_url: Original full URL
        """
        pass


class ABFSParser(StoragePathParser):
    """Parser for Azure Data Lake Storage Gen2 ABFS/ABFSS URLs"""
    
    # Pattern: abfs://container@account.dfs.core.windows.net/path
    # Pattern: abfss://container@account.dfs.core.windows.net/path (secure)
    ABFS_PATTERN = re.compile(
        r'^(abfs|abfss)://([^@]+)@([^.]+)\.dfs\.core\.windows\.net(.*)$',
        re.IGNORECASE
    )
    
    def can_parse(self, path: str) -> bool:
        """Check if path is an ABFS/ABFSS URL"""
        if not path:
            return False
        return bool(self.ABFS_PATTERN.match(path.strip()))
    
    def parse(self, path: str) -> Dict:
        """
        Parse ABFS/ABFSS URL
        
        Example:
            abfs://lh-enriched@hblazlakehousepreprdstg1.dfs.core.windows.net/visionplus/ATH3
            -> {
                "type": "azure_datalake",
                "account_name": "hblazlakehousepreprdstg1",
                "container": "lh-enriched",
                "path": "visionplus/ATH3",
                "protocol": "abfs",
                "full_url": "abfs://..."
            }
        """
        path = path.strip()
        match = self.ABFS_PATTERN.match(path)
        
        if not match:
            raise ValueError(f"Invalid ABFS URL format: {path}")
        
        protocol = match.group(1).lower()
        container = match.group(2)
        account_name = match.group(3)
        file_path = match.group(4).lstrip('/')  # Remove leading slash
        
        return {
            "type": "azure_datalake",
            "account_name": account_name,
            "container": container,
            "path": file_path,
            "protocol": protocol,
            "full_url": path,
            "connection": {
                "method": "service_principal",  # ABFS requires service principal
                "account_name": account_name,
                "endpoint": f"https://{account_name}.dfs.core.windows.net"
            },
            "container_info": {
                "name": container,
                "type": "filesystem"  # Data Lake Gen2 uses filesystem terminology
            },
            "metadata": {
                "storage_type": "azure_datalake_gen2",
                "protocol": protocol
            }
        }


class AzureBlobParser(StoragePathParser):
    """Parser for standard Azure Blob Storage paths"""
    
    # Pattern: https://account.blob.core.windows.net/container/path
    BLOB_URL_PATTERN = re.compile(
        r'^https://([^.]+)\.blob\.core\.windows\.net/([^/]+)(.*)$',
        re.IGNORECASE
    )
    
    def can_parse(self, path: str) -> bool:
        """Check if path is an Azure Blob URL or simple container/path format"""
        if not path:
            return False
        
        # Check for blob URL format
        if self.BLOB_URL_PATTERN.match(path.strip()):
            return True
        
        # Check for simple container/path format (no protocol)
        # This is the most common format used in the codebase
        if '/' in path and not path.startswith(('http://', 'https://', 'abfs://', 'abfss://', 's3://', 'gs://')):
            return True
        
        return False
    
    def parse(self, path: str, account_name: Optional[str] = None, 
              container: Optional[str] = None) -> Dict:
        """
        Parse Azure Blob Storage path
        
        Supports:
        1. Full URL: https://account.blob.core.windows.net/container/path/file.csv
        2. Simple path: container/path/file.csv (requires account_name and container)
        
        Args:
            path: Path to parse
            account_name: Account name (required for simple paths)
            container: Container name (required for simple paths)
        """
        path = path.strip()
        
        # Try full URL format first
        match = self.BLOB_URL_PATTERN.match(path)
        if match:
            account_name = match.group(1)
            container = match.group(2)
            file_path = match.group(3).lstrip('/')
        else:
            # Simple path format
            if account_name and container:
                # If account and container are provided, treat path as relative to container
                file_path = path
            elif not account_name and not container:
                # If neither provided, try to extract from path
                # Format: container/path/file.csv
                parts = path.split('/', 1)
                if len(parts) == 2:
                    container = parts[0]
                    file_path = parts[1]
                else:
                    # Just the file path, no container info
                    file_path = path
                    container = None
            else:
                # One of account_name or container provided, use path as-is
                file_path = path
        
        return {
            "type": "azure_blob",
            "account_name": account_name or "unknown",
            "container": container or "unknown",
            "path": file_path,
            "protocol": "https",
            "full_url": f"https://{account_name}.blob.core.windows.net/{container}/{file_path}" if account_name and container else path,
            "connection": {
                "method": "connection_string",  # Default, can be overridden
                "account_name": account_name or "unknown"
            },
            "container_info": {
                "name": container or "unknown",
                "type": "blob_container"
            },
            "metadata": {
                "storage_type": "azure_blob_storage"
            }
        }


class PathParserRegistry:
    """Registry for storage path parsers with auto-detection"""
    
    def __init__(self):
        self.parsers: list[StoragePathParser] = []
        self._register_default_parsers()
    
    def _register_default_parsers(self):
        """Register default parsers in priority order"""
        # ABFS parser first (most specific)
        self.register(ABFSParser())
        # Azure Blob parser (handles URLs and simple paths)
        self.register(AzureBlobParser())
    
    def register(self, parser: StoragePathParser):
        """Register a new parser"""
        if parser not in self.parsers:
            self.parsers.append(parser)
            logger.info(f'FN:PathParserRegistry.register parser:{parser.__class__.__name__}')
    
    def parse(self, path: str, account_name: Optional[str] = None, 
              container: Optional[str] = None) -> Dict:
        """
        Parse a storage path using registered parsers
        
        Args:
            path: Storage path/URL to parse
            account_name: Optional account name (for simple paths)
            container: Optional container name (for simple paths)
        
        Returns:
            Parsed storage location dictionary
        
        Raises:
            ValueError: If no parser can handle the path
        """
        if not path:
            raise ValueError("Path cannot be empty")
        
        # Try each parser in order
        for parser in self.parsers:
            if parser.can_parse(path):
                try:
                    # Special handling for AzureBlobParser which needs account/container
                    if isinstance(parser, AzureBlobParser):
                        result = parser.parse(path, account_name, container)
                    else:
                        result = parser.parse(path)
                    
                    logger.info(f'FN:PathParserRegistry.parse path:{path} parser:{parser.__class__.__name__} type:{result.get("type")}')
                    return result
                except Exception as e:
                    logger.warning(f'FN:PathParserRegistry.parse path:{path} parser:{parser.__class__.__name__} error:{str(e)}')
                    continue
        
        # If no parser matched, try AzureBlobParser as fallback for simple paths
        if account_name and container:
            try:
                parser = AzureBlobParser()
                result = parser.parse(path, account_name, container)
                logger.info(f'FN:PathParserRegistry.parse path:{path} parser:AzureBlobParser(fallback) type:{result.get("type")}')
                return result
            except Exception as e:
                logger.warning(f'FN:PathParserRegistry.parse path:{path} parser:AzureBlobParser(fallback) error:{str(e)}')
        
        raise ValueError(f"No parser found for path: {path}")


# Global registry instance
_default_registry = PathParserRegistry()


def parse_storage_path(path: str, account_name: Optional[str] = None, 
                      container: Optional[str] = None) -> Dict:
    """
    Convenience function to parse storage paths using the default registry
    
    Args:
        path: Storage path/URL to parse
        account_name: Optional account name (for simple paths)
        container: Optional container name (for simple paths)
    
    Returns:
        Parsed storage location dictionary
    """
    return _default_registry.parse(path, account_name, container)

