"""
ML-based Lineage Relationship Inference
Uses fuzzy matching and pattern detection to infer relationships
"""
import logging
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher
import re

logger = logging.getLogger(__name__)

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    logger.warning('FN:ml_lineage_inference sklearn_not_available:{}'.format(True))


def fuzzy_column_match(column1: str, column2: str, threshold: float = 0.8) -> Tuple[bool, float]:
    """
    Fuzzy match two column names using multiple similarity metrics
    
    Args:
        column1: First column name
        column2: Second column name
        threshold: Minimum similarity score (0.0 to 1.0)
    
    Returns:
        Tuple of (is_match, similarity_score)
    """
    if not column1 or not column2:
        return False, 0.0
    
    col1_lower = column1.lower().strip()
    col2_lower = column2.lower().strip()
    
    # Exact match
    if col1_lower == col2_lower:
        return True, 1.0
    
    # Normalize column names (remove special chars, underscores)
    col1_norm = re.sub(r'[_\-\s]', '', col1_lower)
    col2_norm = re.sub(r'[_\-\s]', '', col2_lower)
    
    if col1_norm == col2_norm:
        return True, 0.95
    
    # Sequence similarity (Ratcliff-Obershelp)
    similarity = SequenceMatcher(None, col1_lower, col2_lower).ratio()
    
    # Check for substring matches (e.g., "user_id" vs "userid")
    if col1_norm in col2_norm or col2_norm in col1_norm:
        similarity = max(similarity, 0.85)
    
    # Check for common patterns
    # Remove common prefixes/suffixes
    prefixes = ['tbl_', 'dim_', 'fact_', 'stg_', 'raw_', 'src_']
    suffixes = ['_id', '_key', '_pk', '_fk']
    
    col1_clean = col1_lower
    col2_clean = col2_lower
    for prefix in prefixes:
        if col1_clean.startswith(prefix):
            col1_clean = col1_clean[len(prefix):]
        if col2_clean.startswith(prefix):
            col2_clean = col2_clean[len(prefix):]
    
    for suffix in suffixes:
        if col1_clean.endswith(suffix):
            col1_clean = col1_clean[:-len(suffix)]
        if col2_clean.endswith(suffix):
            col2_clean = col2_clean[:-len(suffix)]
    
    if col1_clean == col2_clean:
        similarity = max(similarity, 0.9)
    
    is_match = similarity >= threshold
    return is_match, similarity


def infer_relationships_ml(
    source_columns: List[Dict],
    target_columns: List[Dict],
    min_matching_ratio: float = 0.3
) -> Tuple[List[Dict], float]:
    """
    Use ML-based inference to find column relationships
    
    Args:
        source_columns: List of source column dicts with 'name' key
        target_columns: List of target column dicts with 'name' key
        min_matching_ratio: Minimum ratio of columns that must match
    
    Returns:
        Tuple of (column_lineage_list, confidence_score)
    """
    if not source_columns or not target_columns:
        return [], 0.0
    
    source_col_names = [col.get('name', '') for col in source_columns if col.get('name')]
    target_col_names = [col.get('name', '') for col in target_columns if col.get('name')]
    
    if not source_col_names or not target_col_names:
        return [], 0.0
    
    # Find matches using fuzzy matching
    column_lineage = []
    matched_source = set()
    matched_target = set()
    total_similarity = 0.0
    match_count = 0
    
    for source_col in source_col_names:
        best_match = None
        best_score = 0.0
        
        for target_col in target_col_names:
            if target_col in matched_target:
                continue
            
            is_match, similarity = fuzzy_column_match(source_col, target_col, threshold=0.6)
            
            if is_match and similarity > best_score:
                best_match = target_col
                best_score = similarity
        
        if best_match and best_score >= 0.6:
            column_lineage.append({
                'source_column': source_col,
                'target_column': best_match,
                'transformation': 'pass_through',
                'transformation_type': 'pass_through',
                'similarity_score': best_score
            })
            matched_source.add(source_col)
            matched_target.add(best_match)
            total_similarity += best_score
            match_count += 1
    
    # Calculate confidence based on match ratio and similarity scores
    if match_count == 0:
        return [], 0.0
    
    avg_similarity = total_similarity / match_count
    match_ratio = match_count / max(len(source_col_names), len(target_col_names))
    
    # Confidence combines match ratio and average similarity
    confidence = (match_ratio * 0.6) + (avg_similarity * 0.4)
    
    # Boost confidence if match ratio is high
    if match_ratio >= min_matching_ratio:
        confidence = min(0.95, confidence + 0.1)
    
    logger.info('FN:infer_relationships_ml source_cols:{} target_cols:{} matches:{} confidence:{}'.format(
        len(source_col_names), len(target_col_names), match_count, confidence
    ))
    
    return column_lineage, confidence


def detect_transformation_pattern(source_col: str, target_col: str) -> Dict:
    """
    Detect transformation patterns between columns
    
    Returns:
        Dict with transformation_type and transformation_description
    """
    source_lower = source_col.lower()
    target_lower = target_col.lower()
    
    # Aggregation patterns
    if any(agg in target_lower for agg in ['sum_', 'avg_', 'count_', 'max_', 'min_', 'total_']):
        if source_lower in target_lower or target_lower.replace('sum_', '').replace('avg_', '').replace('count_', '').replace('max_', '').replace('min_', '').replace('total_', '') == source_lower:
            return {
                'transformation_type': 'aggregate',
                'transformation': 'aggregation'
            }
    
    # Renaming patterns
    if source_lower != target_lower:
        similarity = SequenceMatcher(None, source_lower, target_lower).ratio()
        if similarity > 0.7:
            return {
                'transformation_type': 'rename',
                'transformation': 'column_rename'
            }
    
    # Default: pass through
    return {
        'transformation_type': 'pass_through',
        'transformation': 'pass_through'
    }


