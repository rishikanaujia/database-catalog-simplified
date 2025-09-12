"""Column classification configuration loader - Refactored to use BaseConfig"""

from typing import Dict, List, Any
from src.core.base_config import BaseConfig

class ColumnClassificationConfig(BaseConfig):
    """Loads and manages column classification rules using BaseConfig"""
    
    def __init__(self, config_path: str = "config/column_classification.yaml"):
        super().__init__(config_path, "column classification")
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Fallback to hardcoded defaults if config file is missing"""
        return {
            'primary_keys': {
                'suffixes': ['_sk', '_id', '_key', '_pk'],
                'requires_not_null': True
            },
            'foreign_keys': {
                'suffixes': ['_sk', '_fk', '_ref_id', '_foreign_key'],
                'requires_not_null': False
            },
            'measures': {
                'amount_fields': {
                    'keywords': ['amount', 'price', 'cost', 'total', 'value', 'revenue', 'sales', 'fee', 'charge'],
                    'business_type': 'Currency'
                },
                'quantity_fields': {
                    'keywords': ['quantity', 'qty', 'count', 'num', 'units', 'volume', 'weight'],
                    'business_type': 'Quantity'
                }
            },
            'dimensions': {
                'description_fields': {
                    'keywords': ['name', 'desc', 'description', 'title', 'label', 'comment'],
                    'business_type': 'Description'
                },
                'status_fields': {
                    'keywords': ['status', 'state', 'flag', 'indicator', 'type', 'category'],
                    'business_type': 'Status'
                },
                'location_fields': {
                    'keywords': ['address', 'city', 'state', 'country', 'zip', 'postal', 'region'],
                    'business_type': 'Location'
                }
            },
            'default_business_types': {
                'DATE': 'Date',
                'DATETIME': 'Date',
                'TIMESTAMP': 'Date',
                'TIME': 'Date',
                'TEXT': 'Text',
                'VARCHAR': 'Text',
                'CHAR': 'Text',
                'STRING': 'Text',
                'NUMBER': 'Numeric',
                'INTEGER': 'Numeric',
                'BIGINT': 'Numeric',
                'DECIMAL': 'Numeric',
                'FLOAT': 'Numeric',
                'BOOLEAN': 'Boolean'
            }
        }
    
    # Primary key properties
    @property
    def primary_key_suffixes(self) -> List[str]:
        return self.get('primary_keys.suffixes', ['_sk', '_id', '_key', '_pk'])
    
    @property
    def primary_key_requires_not_null(self) -> bool:
        return self.get('primary_keys.requires_not_null', True)
    
    # Foreign key properties
    @property
    def foreign_key_suffixes(self) -> List[str]:
        return self.get('foreign_keys.suffixes', ['_sk', '_fk', '_ref_id', '_foreign_key'])
    
    # Measure field properties
    @property
    def amount_keywords(self) -> List[str]:
        return self.get('measures.amount_fields.keywords', 
                       ['amount', 'price', 'cost', 'total', 'value', 'revenue', 'sales', 'fee', 'charge'])
    
    @property
    def quantity_keywords(self) -> List[str]:
        return self.get('measures.quantity_fields.keywords', 
                       ['quantity', 'qty', 'count', 'num', 'units', 'volume', 'weight'])
    
    # Dimension field properties
    @property
    def description_keywords(self) -> List[str]:
        return self.get('dimensions.description_fields.keywords', 
                       ['name', 'desc', 'description', 'title', 'label', 'comment'])
    
    @property
    def status_keywords(self) -> List[str]:
        return self.get('dimensions.status_fields.keywords', 
                       ['status', 'state', 'flag', 'indicator', 'type', 'category'])
    
    @property
    def location_keywords(self) -> List[str]:
        return self.get('dimensions.location_fields.keywords', 
                       ['address', 'city', 'state', 'country', 'zip', 'postal', 'region'])
    
    # Business type getters
    def get_business_type_for_sql_type(self, sql_type: str) -> str:
        """Get default business type for SQL data type"""
        default_types = self.get('default_business_types', {})
        return default_types.get(sql_type.upper(), 'Text')
    
    def get_amount_business_type(self) -> str:
        return self.get('measures.amount_fields.business_type', 'Currency')
    
    def get_quantity_business_type(self) -> str:
        return self.get('measures.quantity_fields.business_type', 'Quantity')
    
    def get_description_business_type(self) -> str:
        return self.get('dimensions.description_fields.business_type', 'Description')
    
    def get_status_business_type(self) -> str:
        return self.get('dimensions.status_fields.business_type', 'Status')
    
    def get_location_business_type(self) -> str:
        return self.get('dimensions.location_fields.business_type', 'Location')
    
    # Additional helper methods
    def get_all_measure_keywords(self) -> List[str]:
        """Get all keywords that indicate measure fields"""
        return self.amount_keywords + self.quantity_keywords
    
    def get_all_dimension_keywords(self) -> List[str]:
        """Get all keywords that indicate dimension fields"""
        return self.description_keywords + self.status_keywords + self.location_keywords
    
    def get_all_key_suffixes(self) -> List[str]:
        """Get all suffixes that indicate key fields"""
        return self.primary_key_suffixes + self.foreign_key_suffixes
    
    def classify_field_by_name(self, field_name: str) -> Dict[str, Any]:
        """Classify a field based on its name and return classification info"""
        field_lower = field_name.lower()
        
        # Check primary keys
        for suffix in self.primary_key_suffixes:
            if field_lower.endswith(suffix):
                return {
                    'role': 'primary_key',
                    'business_type': 'Identifier',
                    'reason': f'Ends with primary key suffix: {suffix}'
                }
        
        # Check foreign keys
        for suffix in self.foreign_key_suffixes:
            if field_lower.endswith(suffix):
                return {
                    'role': 'foreign_key',
                    'business_type': 'Identifier',
                    'reason': f'Ends with foreign key suffix: {suffix}'
                }
        
        # Check amount fields
        for keyword in self.amount_keywords:
            if keyword in field_lower:
                return {
                    'role': 'measure',
                    'business_type': self.get_amount_business_type(),
                    'reason': f'Contains amount keyword: {keyword}'
                }
        
        # Check quantity fields
        for keyword in self.quantity_keywords:
            if keyword in field_lower:
                return {
                    'role': 'measure',
                    'business_type': self.get_quantity_business_type(),
                    'reason': f'Contains quantity keyword: {keyword}'
                }
        
        # Check description fields
        for keyword in self.description_keywords:
            if keyword in field_lower:
                return {
                    'role': 'dimension',
                    'business_type': self.get_description_business_type(),
                    'reason': f'Contains description keyword: {keyword}'
                }
        
        # Check status fields
        for keyword in self.status_keywords:
            if keyword in field_lower:
                return {
                    'role': 'dimension',
                    'business_type': self.get_status_business_type(),
                    'reason': f'Contains status keyword: {keyword}'
                }
        
        # Check location fields
        for keyword in self.location_keywords:
            if keyword in field_lower:
                return {
                    'role': 'dimension',
                    'business_type': self.get_location_business_type(),
                    'reason': f'Contains location keyword: {keyword}'
                }
        
        # Default classification
        return {
            'role': 'dimension',
            'business_type': 'Text',
            'reason': 'No specific patterns detected'
        }
    
    def validate(self) -> bool:
        """Validate configuration values"""
        try:
            # Validate that required sections exist
            required_sections = ['primary_keys', 'foreign_keys', 'measures', 'dimensions', 'default_business_types']
            for section in required_sections:
                if not self.get(section):
                    return False
            
            # Validate that lists are not empty
            if not self.primary_key_suffixes:
                return False
            if not self.amount_keywords:
                return False
            if not self.description_keywords:
                return False
            
            # Validate business types are strings
            for business_type in [self.get_amount_business_type(), self.get_quantity_business_type(),
                                 self.get_description_business_type(), self.get_status_business_type()]:
                if not isinstance(business_type, str) or not business_type.strip():
                    return False
            
            return True
        except Exception:
            return False

# Global instance
COLUMN_CONFIG = ColumnClassificationConfig()