import os
import logging
from typing import Dict, List, Optional
from azure.core.credentials import AzureKeyCredential
from azure.ai.textanalytics import TextAnalyticsClient
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Azure AI Language (PII Detection) configuration
AZURE_AI_LANGUAGE_ENDPOINT = os.getenv("AZURE_AI_LANGUAGE_ENDPOINT", "")
AZURE_AI_LANGUAGE_KEY = os.getenv("AZURE_AI_LANGUAGE_KEY", "")


class AzureDLPClient:
    """Client for Azure AI Language PII detection"""
    
    def __init__(self, endpoint: Optional[str] = None, key: Optional[str] = None):
        self.endpoint = endpoint or AZURE_AI_LANGUAGE_ENDPOINT
        self.key = key or AZURE_AI_LANGUAGE_KEY
        
        if not self.endpoint or not self.key:
            logger.warning('FN:AzureDLPClient.__init__ endpoint:{} key_configured:{}'.format(self.endpoint, bool(self.key)))
            self.client = None
        else:
            try:
                # Remove trailing slash if present (TextAnalyticsClient expects no trailing slash)
                endpoint_clean = self.endpoint.rstrip('/')
                credential = AzureKeyCredential(self.key)
                self.client = TextAnalyticsClient(endpoint=endpoint_clean, credential=credential)
                logger.info('FN:AzureDLPClient.__init__ endpoint:{}'.format(endpoint_clean))
            except Exception as e:
                logger.error('FN:AzureDLPClient.__init__ endpoint:{} error:{}'.format(self.endpoint, str(e)))
                self.client = None
    
    def detect_pii_in_text(self, text: str, language: str = "en") -> Dict:
        """
        Detect PII in a text string using Azure AI Language service
        
        Args:
            text: Text to analyze
            language: Language code (default: "en")
        
        Returns:
            Dict with pii_detected (bool), pii_types (list), and confidence scores
        """
        if not self.client or not text:
            return {
                "pii_detected": False,
                "pii_types": [],
                "entities": []
            }
        
        try:
            # Azure AI Language API has a limit of 5120 characters per document
            # For column names, this should be fine, but we'll truncate if needed
            text_to_analyze = text[:5120] if len(text) > 5120 else text
            
            # Call PII detection API
            result = self.client.recognize_pii_entities([text_to_analyze], language=language)
            
            if not result or len(result) == 0:
                return {
                    "pii_detected": False,
                    "pii_types": [],
                    "entities": []
                }
            
            document_result = result[0]
            
            # Check for errors
            if document_result.is_error:
                logger.warning('FN:detect_pii_in_text text_length:{} language:{} error:{}'.format(len(text_to_analyze), language, document_result.error))
                return {
                    "pii_detected": False,
                    "pii_types": [],
                    "entities": []
                }
            
            # Extract PII entities
            entities = []
            pii_types = []
            
            for entity in document_result.entities:
                entities.append({
                    "text": entity.text,
                    "category": entity.category,
                    "subcategory": entity.subcategory,
                    "confidence_score": entity.confidence_score,
                    "offset": entity.offset,
                    "length": entity.length
                })
                
                # Add category to pii_types if not already present
                category = entity.subcategory or entity.category
                if category and category not in pii_types:
                    pii_types.append(category)
            
            return {
                "pii_detected": len(entities) > 0,
                "pii_types": pii_types,
                "entities": entities
            }
            
        except Exception as e:
            logger.error('FN:detect_pii_in_text text_length:{} language:{} error:{}'.format(len(text) if text else 0, language, str(e)))
            return {
                "pii_detected": False,
                "pii_types": [],
                "entities": []
            }
    
    def detect_pii_in_column_name(self, column_name: str, sample_data: Optional[List[str]] = None) -> Dict:
        """
        Detect PII in a column name and/or sample data using Azure DLP
        
        Args:
            column_name: Column name to analyze
            sample_data: Optional list of sample values from the column
        
        Returns:
            Dict with pii_detected (bool) and pii_types (list)
        """
        if not column_name:
            return {
                "pii_detected": False,
                "pii_types": []
            }
        
        # Use Azure DLP to detect PII
        if not self.client:
            logger.warning('FN:detect_pii_in_column_name column_name:{} message:Azure DLP client not configured'.format(column_name))
            return {
                "pii_detected": False,
                "pii_types": []
            }
        
        all_pii_types = []
        pii_detected = False
        
        # 1. Analyze the column name itself
        name_result = self.detect_pii_in_text(column_name)
        if name_result.get("pii_detected"):
            pii_detected = True
            all_pii_types.extend(name_result.get("pii_types", []))
        
        # 2. Analyze sample data from the column (if available)
        if sample_data:
            # Combine sample values into a text string for analysis
            # Azure DLP can analyze up to 5120 characters per document
            sample_text = " ".join(str(val) for val in sample_data[:10])  # Analyze first 10 samples
            sample_text = sample_text[:5120]  # Truncate if too long
            
            if sample_text.strip():
                sample_result = self.detect_pii_in_text(sample_text)
                if sample_result.get("pii_detected"):
                    pii_detected = True
                    # Add new PII types found in sample data
                    for pii_type in sample_result.get("pii_types", []):
                        if pii_type not in all_pii_types:
                            all_pii_types.append(pii_type)
        
        return {
            "pii_detected": pii_detected,
            "pii_types": list(set(all_pii_types))  # Remove duplicates
        }


# Global instance (initialized on first use)
_dlp_client = None


def get_dlp_client() -> Optional[AzureDLPClient]:
    """Get or create the global Azure DLP client instance"""
    global _dlp_client
    if _dlp_client is None:
        _dlp_client = AzureDLPClient()
    return _dlp_client


def detect_pii_in_column(column_name: str, sample_data: Optional[List[str]] = None) -> Dict:
    """
    Convenience function to detect PII in a column name and/or sample data using Azure DLP
    
    Args:
        column_name: Column name to analyze
        sample_data: Optional list of sample values from the column
    
    Returns:
        Dict with pii_detected (bool) and pii_types (list)
    """
    client = get_dlp_client()
    if not client:
        return {
            "pii_detected": False,
            "pii_types": []
        }
    return client.detect_pii_in_column_name(column_name, sample_data)
