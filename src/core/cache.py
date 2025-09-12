"""Caching layer for database catalog to improve performance"""

import json
import hashlib
import pickle
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional, Dict, Union, Callable
from functools import wraps
import pandas as pd

logger = logging.getLogger("database_catalog")

class TTLCache:
    """Time-to-live cache with automatic expiration"""
    
    def __init__(self, ttl_seconds: int = 3600, max_size: int = 1000):
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self.cache: Dict[str, Dict] = {}
        self._access_times: Dict[str, float] = {}
    
    def _is_expired(self, key: str) -> bool:
        """Check if a cache entry is expired"""
        if key not in self.cache:
            return True
        
        entry_time = self.cache[key]['timestamp']
        return (time.time() - entry_time) > self.ttl_seconds
    
    def _evict_expired(self):
        """Remove expired entries from cache"""
        current_time = time.time()
        expired_keys = []
        
        for key, data in self.cache.items():
            if (current_time - data['timestamp']) > self.ttl_seconds:
                expired_keys.append(key)
        
        for key in expired_keys:
            self.cache.pop(key, None)
            self._access_times.pop(key, None)
    
    def _evict_lru(self):
        """Evict least recently used items if cache is full"""
        if len(self.cache) <= self.max_size:
            return
        
        # Sort by access time and remove oldest
        sorted_keys = sorted(self._access_times.items(), key=lambda x: x[1])
        num_to_remove = len(self.cache) - self.max_size + 1
        
        for key, _ in sorted_keys[:num_to_remove]:
            self.cache.pop(key, None)
            self._access_times.pop(key, None)
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired"""
        self._evict_expired()
        
        if key in self.cache and not self._is_expired(key):
            self._access_times[key] = time.time()
            return self.cache[key]['value']
        
        return None
    
    def set(self, key: str, value: Any):
        """Set value in cache with current timestamp"""
        self._evict_expired()
        self._evict_lru()
        
        self.cache[key] = {
            'value': value,
            'timestamp': time.time()
        }
        self._access_times[key] = time.time()
    
    def delete(self, key: str):
        """Remove specific key from cache"""
        self.cache.pop(key, None)
        self._access_times.pop(key, None)
    
    def clear(self):
        """Clear all cache entries"""
        self.cache.clear()
        self._access_times.clear()
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        current_time = time.time()
        expired_count = sum(1 for data in self.cache.values() 
                          if (current_time - data['timestamp']) > self.ttl_seconds)
        
        return {
            'size': len(self.cache),
            'max_size': self.max_size,
            'ttl_seconds': self.ttl_seconds,
            'expired_entries': expired_count,
            'hit_ratio': getattr(self, '_hit_count', 0) / max(getattr(self, '_request_count', 1), 1)
        }

class FileCache:
    """Disk-based cache for larger objects like DataFrames"""
    
    def __init__(self, cache_dir: str = "./cache", ttl_hours: int = 24):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_hours = ttl_hours
        self.metadata_file = self.cache_dir / "cache_metadata.json"
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> Dict:
        """Load cache metadata from disk"""
        if self.metadata_file.exists():
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load cache metadata: {e}")
        return {}
    
    def _save_metadata(self):
        """Save cache metadata to disk"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f)
        except Exception as e:
            logger.warning(f"Could not save cache metadata: {e}")
    
    def _get_cache_key(self, key: str) -> str:
        """Generate filesystem-safe cache key"""
        return hashlib.md5(key.encode()).hexdigest()
    
    def _is_expired(self, cache_key: str) -> bool:
        """Check if cached file is expired"""
        if cache_key not in self.metadata:
            return True
        
        cache_time = datetime.fromisoformat(self.metadata[cache_key]['timestamp'])
        return datetime.now() - cache_time > timedelta(hours=self.ttl_hours)
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached object from disk"""
        cache_key = self._get_cache_key(key)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        if not cache_file.exists() or self._is_expired(cache_key):
            return None
        
        try:
            with open(cache_file, 'rb') as f:
                data = pickle.load(f)
            
            # Update access time
            self.metadata[cache_key]['last_accessed'] = datetime.now().isoformat()
            self._save_metadata()
            
            logger.debug(f"Cache hit for key: {key}")
            return data
            
        except Exception as e:
            logger.warning(f"Error reading cache file for key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, metadata: Optional[Dict] = None):
        """Cache object to disk"""
        cache_key = self._get_cache_key(key)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        try:
            with open(cache_file, 'wb') as f:
                pickle.dump(value, f)
            
            # Save metadata
            self.metadata[cache_key] = {
                'original_key': key,
                'timestamp': datetime.now().isoformat(),
                'last_accessed': datetime.now().isoformat(),
                'size_bytes': cache_file.stat().st_size if cache_file.exists() else 0,
                'metadata': metadata or {}
            }
            self._save_metadata()
            
            logger.debug(f"Cached data for key: {key}")
            
        except Exception as e:
            logger.error(f"Error caching data for key {key}: {e}")
    
    def delete(self, key: str):
        """Delete cached file"""
        cache_key = self._get_cache_key(key)
        cache_file = self.cache_dir / f"{cache_key}.pkl"
        
        try:
            if cache_file.exists():
                cache_file.unlink()
            self.metadata.pop(cache_key, None)
            self._save_metadata()
        except Exception as e:
            logger.warning(f"Error deleting cache for key {key}: {e}")
    
    def clear_expired(self):
        """Remove all expired cache files"""
        expired_keys = []
        
        for cache_key in list(self.metadata.keys()):
            if self._is_expired(cache_key):
                expired_keys.append(cache_key)
                cache_file = self.cache_dir / f"{cache_key}.pkl"
                try:
                    if cache_file.exists():
                        cache_file.unlink()
                except Exception as e:
                    logger.warning(f"Error deleting expired cache file: {e}")
        
        for key in expired_keys:
            self.metadata.pop(key, None)
        
        if expired_keys:
            self._save_metadata()
            logger.info(f"Cleared {len(expired_keys)} expired cache entries")
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_size = sum(entry.get('size_bytes', 0) for entry in self.metadata.values())
        expired_count = sum(1 for key in self.metadata.keys() if self._is_expired(key))
        
        return {
            'entries': len(self.metadata),
            'total_size_mb': total_size / (1024 * 1024),
            'expired_entries': expired_count,
            'ttl_hours': self.ttl_hours,
            'cache_dir': str(self.cache_dir)
        }

class CacheManager:
    """Unified cache manager for the database catalog"""
    
    def __init__(self):
        self.memory_cache = TTLCache(ttl_seconds=3600, max_size=500)
        self.file_cache = FileCache(cache_dir="./cache", ttl_hours=24)
        
    def get_query_cache_key(self, query: str, params: Optional[list] = None) -> str:
        """Generate cache key for database queries"""
        key_data = {
            'query': query.strip(),
            'params': params or [],
            'type': 'query'
        }
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()
    
    def get_profiling_cache_key(self, table_name: str, column_name: str) -> str:
        """Generate cache key for column profiling data"""
        key_data = {
            'table': table_name,
            'column': column_name,
            'type': 'profiling'
        }
        return hashlib.md5(json.dumps(key_data, sort_keys=True).encode()).hexdigest()
    
    def cache_query_result(self, query: str, result: Any, params: Optional[list] = None):
        """Cache database query result"""
        cache_key = self.get_query_cache_key(query, params)
        
        # Use memory cache for small results, file cache for large ones
        if isinstance(result, list) and len(result) < 100:
            self.memory_cache.set(cache_key, result)
        else:
            self.file_cache.set(cache_key, result, {'query_length': len(query)})
    
    def get_cached_query_result(self, query: str, params: Optional[list] = None) -> Optional[Any]:
        """Get cached database query result"""
        cache_key = self.get_query_cache_key(query, params)
        
        # Try memory cache first
        result = self.memory_cache.get(cache_key)
        if result is not None:
            return result
        
        # Try file cache
        return self.file_cache.get(cache_key)
    
    def clear_all_caches(self):
        """Clear both memory and file caches"""
        self.memory_cache.clear()
        self.file_cache.clear_expired()
        logger.info("Cleared all caches")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        return {
            'memory_cache': self.memory_cache.stats(),
            'file_cache': self.file_cache.stats()
        }

# Global cache manager instance
CACHE_MANAGER = CacheManager()

def cached_query(ttl_seconds: int = 3600):
    """Decorator for caching database query results"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key_data = {
                'func': func.__name__,
                'args': str(args),
                'kwargs': str(sorted(kwargs.items()))
            }
            cache_key = hashlib.md5(json.dumps(cache_key_data, sort_keys=True).encode()).hexdigest()
            
            # Try to get from cache
            cached_result = CACHE_MANAGER.memory_cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            CACHE_MANAGER.memory_cache.set(cache_key, result)
            logger.debug(f"Cached result for {func.__name__}")
            
            return result
        
        return wrapper
    return decorator

def cached_dataframe(ttl_hours: int = 24):
    """Decorator for caching DataFrame results to disk"""
    def decorator(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Create cache key from function name and arguments
            cache_key_data = {
                'func': func.__name__,
                'args': str(args),
                'kwargs': str(sorted(kwargs.items()))
            }
            cache_key = hashlib.md5(json.dumps(cache_key_data, sort_keys=True).encode()).hexdigest()
            
            # Try to get from cache
            cached_result = CACHE_MANAGER.file_cache.get(cache_key)
            if cached_result is not None:
                logger.debug(f"DataFrame cache hit for {func.__name__}")
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            CACHE_MANAGER.file_cache.set(cache_key, result, 
                                       {'func': func.__name__, 'rows': len(result) if hasattr(result, '__len__') else 0})
            logger.debug(f"Cached DataFrame for {func.__name__}")
            
            return result
        
        return wrapper
    return decorator