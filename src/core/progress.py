"""Progress tracking utilities for database catalog operations"""

import time
import logging
from typing import Optional, Dict, Any, Iterator, Callable
from contextlib import contextmanager

logger = logging.getLogger("database_catalog")

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    logger.info("tqdm not available, using basic progress tracking")

class ProgressTracker:
    """Unified progress tracking with multiple output methods"""
    
    def __init__(self, total: int, description: str = "Processing", 
                 use_tqdm: bool = True, log_interval: int = 10):
        self.total = total
        self.description = description
        self.current = 0
        self.start_time = time.time()
        self.last_log_time = time.time()
        self.log_interval = log_interval  # Log every N seconds
        self.use_tqdm = use_tqdm and TQDM_AVAILABLE
        
        # Initialize progress bar if available
        if self.use_tqdm:
            self.pbar = tqdm(total=total, desc=description, unit="items")
        else:
            self.pbar = None
            logger.info(f"Starting {description}: 0/{total}")
    
    def update(self, increment: int = 1, message: str = None):
        """Update progress by increment"""
        self.current += increment
        
        if self.use_tqdm and self.pbar:
            if message:
                self.pbar.set_postfix_str(message)
            self.pbar.update(increment)
        else:
            # Log-based progress tracking
            current_time = time.time()
            if current_time - self.last_log_time >= self.log_interval or self.current >= self.total:
                elapsed = current_time - self.start_time
                rate = self.current / elapsed if elapsed > 0 else 0
                eta = (self.total - self.current) / rate if rate > 0 else 0
                
                progress_msg = f"{self.description}: {self.current}/{self.total} "
                progress_msg += f"({self.current/self.total*100:.1f}%) "
                progress_msg += f"Rate: {rate:.1f}/s "
                
                if eta < 3600:
                    progress_msg += f"ETA: {eta/60:.1f}m"
                else:
                    progress_msg += f"ETA: {eta/3600:.1f}h"
                
                if message:
                    progress_msg += f" | {message}"
                
                logger.info(progress_msg)
                self.last_log_time = current_time
    
    def set_description(self, description: str):
        """Update the description"""
        self.description = description
        if self.use_tqdm and self.pbar:
            self.pbar.set_description(description)
    
    def set_postfix(self, **kwargs):
        """Set postfix information"""
        if self.use_tqdm and self.pbar:
            self.pbar.set_postfix(**kwargs)
    
    def close(self):
        """Close the progress tracker"""
        if self.use_tqdm and self.pbar:
            self.pbar.close()
        else:
            elapsed = time.time() - self.start_time
            rate = self.current / elapsed if elapsed > 0 else 0
            logger.info(f"Completed {self.description}: {self.current}/{self.total} "
                       f"in {elapsed:.1f}s (avg {rate:.1f}/s)")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

class OperationTimer:
    """Timer for tracking operation durations"""
    
    def __init__(self, operation_name: str, log_start: bool = True):
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
        
        if log_start:
            logger.info(f"Starting {operation_name}...")
    
    def start(self):
        """Start the timer"""
        self.start_time = time.time()
        return self
    
    def stop(self):
        """Stop the timer and log duration"""
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if duration < 60:
            logger.info(f"Completed {self.operation_name} in {duration:.1f} seconds")
        elif duration < 3600:
            logger.info(f"Completed {self.operation_name} in {duration/60:.1f} minutes")
        else:
            logger.info(f"Completed {self.operation_name} in {duration/3600:.1f} hours")
        
        return duration
    
    @property
    def duration(self) -> Optional[float]:
        """Get current duration"""
        if self.start_time is None:
            return None
        end_time = self.end_time or time.time()
        return end_time - self.start_time
    
    def __enter__(self):
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

@contextmanager
def progress_tracker(total: int, description: str = "Processing", **kwargs):
    """Context manager for progress tracking"""
    tracker = ProgressTracker(total, description, **kwargs)
    try:
        yield tracker
    finally:
        tracker.close()

@contextmanager  
def operation_timer(operation_name: str):
    """Context manager for operation timing"""
    timer = OperationTimer(operation_name)
    try:
        yield timer.start()
    finally:
        timer.stop()

def track_progress(iterable, description: str = "Processing", **kwargs):
    """Track progress over an iterable"""
    if hasattr(iterable, '__len__'):
        total = len(iterable)
    else:
        # Convert to list to get length (might use more memory)
        iterable = list(iterable)
        total = len(iterable)
    
    with progress_tracker(total, description, **kwargs) as tracker:
        for item in iterable:
            yield item
            tracker.update(1)

class BatchProcessor:
    """Process items in batches with progress tracking"""
    
    def __init__(self, batch_size: int = 10, progress_description: str = "Processing batches"):
        self.batch_size = batch_size
        self.progress_description = progress_description
    
    def process_batches(self, items, processor_func: Callable, **kwargs):
        """Process items in batches with progress tracking"""
        total_items = len(items) if hasattr(items, '__len__') else len(list(items))
        num_batches = (total_items + self.batch_size - 1) // self.batch_size
        
        results = []
        
        with progress_tracker(num_batches, f"{self.progress_description} (batch_size={self.batch_size})", 
                            **kwargs) as tracker:
            
            for i in range(0, total_items, self.batch_size):
                batch = items[i:i + self.batch_size]
                batch_num = i // self.batch_size + 1
                
                tracker.set_postfix(batch=f"{batch_num}/{num_batches}", 
                                   items=f"{min(i + self.batch_size, total_items)}/{total_items}")
                
                try:
                    batch_result = processor_func(batch)
                    if isinstance(batch_result, list):
                        results.extend(batch_result)
                    else:
                        results.append(batch_result)
                        
                except Exception as e:
                    logger.error(f"Error processing batch {batch_num}: {e}")
                    # Could add error handling strategy here
                    continue
                
                tracker.update(1)
        
        return results

class ProgressReporter:
    """Advanced progress reporting with multiple metrics"""
    
    def __init__(self, total_operations: Dict[str, int]):
        self.total_operations = total_operations
        self.completed_operations = {key: 0 for key in total_operations.keys()}
        self.start_time = time.time()
        self.operation_timers = {}
    
    def start_operation(self, operation_name: str):
        """Start tracking a specific operation"""
        self.operation_timers[operation_name] = time.time()
        logger.info(f"Starting operation: {operation_name}")
    
    def complete_operation(self, operation_name: str, count: int = 1):
        """Mark operation as completed"""
        if operation_name not in self.completed_operations:
            logger.warning(f"Unknown operation: {operation_name}")
            return
        
        self.completed_operations[operation_name] += count
        
        # Log completion if operation is finished
        if operation_name in self.operation_timers:
            duration = time.time() - self.operation_timers[operation_name]
            del self.operation_timers[operation_name]
            
            total = self.total_operations[operation_name]
            completed = self.completed_operations[operation_name]
            
            if completed >= total:
                logger.info(f"Completed operation: {operation_name} "
                          f"({completed}/{total}) in {duration:.1f}s")
    
    def get_overall_progress(self) -> Dict[str, Any]:
        """Get overall progress statistics"""
        total_items = sum(self.total_operations.values())
        completed_items = sum(self.completed_operations.values())
        
        elapsed_time = time.time() - self.start_time
        completion_rate = completed_items / elapsed_time if elapsed_time > 0 else 0
        
        eta = (total_items - completed_items) / completion_rate if completion_rate > 0 else 0
        
        return {
            'total_items': total_items,
            'completed_items': completed_items,
            'completion_percentage': (completed_items / total_items * 100) if total_items > 0 else 0,
            'elapsed_time_seconds': elapsed_time,
            'completion_rate_per_second': completion_rate,
            'eta_seconds': eta,
            'operations': {
                op: {
                    'completed': self.completed_operations[op],
                    'total': self.total_operations[op],
                    'percentage': (self.completed_operations[op] / self.total_operations[op] * 100) 
                                if self.total_operations[op] > 0 else 0
                }
                for op in self.total_operations.keys()
            }
        }
    
    def print_progress_report(self):
        """Print a detailed progress report"""
        progress = self.get_overall_progress()
        
        print(f"\n📊 Progress Report")
        print(f"Overall: {progress['completed_items']}/{progress['total_items']} "
              f"({progress['completion_percentage']:.1f}%)")
        print(f"Elapsed: {progress['elapsed_time_seconds']:.1f}s")
        print(f"Rate: {progress['completion_rate_per_second']:.1f} items/sec")
        
        if progress['eta_seconds'] < 3600:
            print(f"ETA: {progress['eta_seconds']/60:.1f} minutes")
        else:
            print(f"ETA: {progress['eta_seconds']/3600:.1f} hours")
        
        print("\nBy Operation:")
        for op_name, op_stats in progress['operations'].items():
            print(f"  {op_name}: {op_stats['completed']}/{op_stats['total']} "
                  f"({op_stats['percentage']:.1f}%)")

# Example decorator for automatic progress tracking
def track_function_progress(description: str = None):
    """Decorator to automatically track function execution progress"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            func_description = description or f"Executing {func.__name__}"
            
            with operation_timer(func_description):
                result = func(*args, **kwargs)
            
            return result
        return wrapper
    return decorator