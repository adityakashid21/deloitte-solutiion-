import json
import asyncio
import concurrent.futures
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from pathlib import Path
import logging
import time
from functools import wraps
import hashlib
import numpy as np
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp

@dataclass
class TelemetryEntry:
    device_id: str
    timestamp: int
    temperature_celsius: float
    pressure_kpa: float
    processing_time: Optional[float] = None
    data_hash: Optional[str] = None

@dataclass
class ProcessingStats:
    total_entries: int
    processed_entries: int
    failed_entries: int
    processing_time: float
    throughput: float
    memory_usage: float

class PerformanceMonitor:
    def __init__(self):
        self.start_time = None
        self.memory_snapshots = []
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        self.duration = self.end_time - self.start_time

def performance_logger(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        end = time.perf_counter()
        logging.info(f"{func.__name__} executed in {end - start:.4f}s")
        return result
    return wrapper

class TelemetryProcessor:
    def __init__(self, max_workers: int = None, batch_size: int = 1000, use_multiprocessing: bool = True):
        self.max_workers = max_workers or min(32, mp.cpu_count() + 4)
        self.batch_size = batch_size
        self.use_multiprocessing = use_multiprocessing
        self.conversion_cache = {}
        self.error_buffer = []
        self.processed_count = 0
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('telemetry_processing.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def generate_data_hash(self, data: Dict[str, Any]) -> str:
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def validate_data_integrity(self, entry: Dict[str, Any], format_type: str) -> bool:
        required_fields = {
            'data1': ['device_id', 'timestamp', 'temp_c', 'pressure_kpa'],
            'data2': ['device_id', 'timestamp', 'temperature_F', 'pressure_PSI']
        }
        
        if format_type not in required_fields:
            return False
        
        return all(field in entry for field in required_fields[format_type])
    
    @performance_logger
    def convert_data1_to_unified(self, data1_entry: Dict[str, Any]) -> Optional[TelemetryEntry]:
        try:
            if not self.validate_data_integrity(data1_entry, 'data1'):
                self.error_buffer.append(f"Invalid data1 entry: {data1_entry}")
                return None
            
            cache_key = self.generate_data_hash(data1_entry)
            if cache_key in self.conversion_cache:
                return self.conversion_cache[cache_key]
            
            start_time = time.perf_counter()
            
            iso_timestamp = data1_entry['timestamp']
            if iso_timestamp.endswith('Z'):
                dt = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
            else:
                dt = datetime.fromisoformat(iso_timestamp)
            
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            
            timestamp_ms = int(dt.timestamp() * 1000)
            
            entry = TelemetryEntry(
                device_id=data1_entry['device_id'],
                timestamp=timestamp_ms,
                temperature_celsius=float(data1_entry['temp_c']),
                pressure_kpa=float(data1_entry['pressure_kpa']),
                processing_time=time.perf_counter() - start_time,
                data_hash=cache_key
            )
            
            self.conversion_cache[cache_key] = entry
            return entry
            
        except Exception as e:
            self.error_buffer.append(f"Error converting data1 entry {data1_entry}: {str(e)}")
            return None
    
    @performance_logger
    def convert_data2_to_unified(self, data2_entry: Dict[str, Any]) -> Optional[TelemetryEntry]:
        try:
            if not self.validate_data_integrity(data2_entry, 'data2'):
                self.error_buffer.append(f"Invalid data2 entry: {data2_entry}")
                return None
            
            cache_key = self.generate_data_hash(data2_entry)
            if cache_key in self.conversion_cache:
                return self.conversion_cache[cache_key]
            
            start_time = time.perf_counter()
            
            timestamp_ms = int(data2_entry['timestamp'])
            temp_f = float(data2_entry['temperature_F'])
            pressure_psi = float(data2_entry['pressure_PSI'])
            
            temp_c = (temp_f - 32) * 5 / 9
            pressure_kpa = pressure_psi * 6.89476
            
            entry = TelemetryEntry(
                device_id=data2_entry['device_id'],
                timestamp=timestamp_ms,
                temperature_celsius=round(temp_c, 3),
                pressure_kpa=round(pressure_kpa, 3),
                processing_time=time.perf_counter() - start_time,
                data_hash=cache_key
            )
            
            self.conversion_cache[cache_key] = entry
            return entry
            
        except Exception as e:
            self.error_buffer.append(f"Error converting data2 entry {data2_entry}: {str(e)}")
            return None
    
    def process_batch(self, batch_data: List[Tuple[Dict[str, Any], str]]) -> List[TelemetryEntry]:
        results = []
        for entry, format_type in batch_data:
            if format_type == 'data1':
                result = self.convert_data1_to_unified(entry)
            elif format_type == 'data2':
                result = self.convert_data2_to_unified(entry)
            else:
                continue
            
            if result:
                results.append(result)
        
        return results
    
    async def process_large_dataset_async(self, file_paths: List[str]) -> Tuple[List[TelemetryEntry], ProcessingStats]:
        start_time = time.perf_counter()
        all_entries = []
        total_entries = 0
        failed_entries = 0
        
        with PerformanceMonitor() as monitor:
            tasks = []
            
            for file_path in file_paths:
                task = asyncio.create_task(self.load_and_process_file_async(file_path))
                tasks.append(task)
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, Exception):
                    self.logger.error(f"Task failed: {result}")
                    failed_entries += 1
                else:
                    entries, count = result
                    all_entries.extend(entries)
                    total_entries += count
        
        processing_time = time.perf_counter() - start_time
        throughput = len(all_entries) / processing_time if processing_time > 0 else 0
        
        stats = ProcessingStats(
            total_entries=total_entries,
            processed_entries=len(all_entries),
            failed_entries=failed_entries,
            processing_time=processing_time,
            throughput=throughput,
            memory_usage=0.0
        )
        
        return all_entries, stats
    
    async def load_and_process_file_async(self, file_path: str) -> Tuple[List[TelemetryEntry], int]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.load_and_process_file, file_path)
    
    def load_and_process_file(self, file_path: str) -> Tuple[List[TelemetryEntry], int]:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not isinstance(data, list):
                data = [data]
            
            format_type = 'data1' if 'temp_c' in str(data[0]) else 'data2'
            batch_data = [(entry, format_type) for entry in data]
            
            batches = [batch_data[i:i + self.batch_size] for i in range(0, len(batch_data), self.batch_size)]
            all_results = []
            
            if self.use_multiprocessing:
                with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [executor.submit(self.process_batch, batch) for batch in batches]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            results = future.result()
                            all_results.extend(results)
                        except Exception as e:
                            self.logger.error(f"Batch processing failed: {e}")
            else:
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [executor.submit(self.process_batch, batch) for batch in batches]
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            results = future.result()
                            all_results.extend(results)
                        except Exception as e:
                            self.logger.error(f"Batch processing failed: {e}")
            
            return all_results, len(data)
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {e}")
            return [], 0
    
    def optimize_and_deduplicate(self, entries: List[TelemetryEntry]) -> List[TelemetryEntry]:
        seen_hashes = set()
        unique_entries = []
        
        for entry in entries:
            if entry.data_hash not in seen_hashes:
                seen_hashes.add(entry.data_hash)
                unique_entries.append(entry)
        
        unique_entries.sort(key=lambda x: (x.device_id, x.timestamp))
        
        self.logger.info(f"Deduplicated {len(entries) - len(unique_entries)} entries")
        return unique_entries
    
    def analyze_telemetry_patterns(self, entries: List[TelemetryEntry]) -> Dict[str, Any]:
        if not entries:
            return {}
        
        temperatures = [e.temperature_celsius for e in entries]
        pressures = [e.pressure_kpa for e in entries]
        
        device_stats = {}
        for entry in entries:
            if entry.device_id not in device_stats:
                device_stats[entry.device_id] = {
                    'count': 0,
                    'temp_readings': [],
                    'pressure_readings': []
                }
            device_stats[entry.device_id]['count'] += 1
            device_stats[entry.device_id]['temp_readings'].append(entry.temperature_celsius)
            device_stats[entry.device_id]['pressure_readings'].append(entry.pressure_kpa)
        
        analysis = {
            'total_entries': len(entries),
            'unique_devices': len(device_stats),
            'temperature_stats': {
                'min': min(temperatures),
                'max': max(temperatures),
                'mean': np.mean(temperatures),
                'std': np.std(temperatures)
            },
            'pressure_stats': {
                'min': min(pressures),
                'max': max(pressures),
                'mean': np.mean(pressures),
                'std': np.std(pressures)
            },
            'device_breakdown': {
                device: {
                    'count': stats['count'],
                    'avg_temp': np.mean(stats['temp_readings']),
                    'avg_pressure': np.mean(stats['pressure_readings'])
                }
                for device, stats in device_stats.items()
            }
        }
        
        return analysis
    
    async def process_enterprise_telemetry(self, input_files: List[str], output_file: str = 'data-result.json') -> Dict[str, Any]:
        self.logger.info(f"Starting enterprise telemetry processing for {len(input_files)} files")
        
        try:
            entries, stats = await self.process_large_dataset_async(input_files)
            
            if not entries:
                self.logger.warning("No valid entries processed")
                return {'status': 'error', 'message': 'No valid entries processed'}
            
            optimized_entries = self.optimize_and_deduplicate(entries)
            analysis = self.analyze_telemetry_patterns(optimized_entries)
            
            output_data = {
                'metadata': {
                    'processing_timestamp': datetime.now(timezone.utc).isoformat(),
                    'total_files_processed': len(input_files),
                    'processing_stats': asdict(stats),
                    'analysis': analysis
                },
                'telemetry_data': [asdict(entry) for entry in optimized_entries]
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Processing complete. Output saved to {output_file}")
            self.logger.info(f"Processed {len(optimized_entries)} unique entries from {stats.total_entries} total entries")
            self.logger.info(f"Processing speed: {stats.throughput:.2f} entries/second")
            
            if self.error_buffer:
                with open('processing_errors.log', 'w') as f:
                    f.write('\n'.join(self.error_buffer))
                self.logger.warning(f"Logged {len(self.error_buffer)} errors to processing_errors.log")
            
            return {
                'status': 'success',
                'processed_entries': len(optimized_entries),
                'processing_time': stats.processing_time,
                'throughput': stats.throughput,
                'output_file': output_file,
                'analysis': analysis
            }
            
        except Exception as e:
            self.logger.error(f"Enterprise processing failed: {e}")
            return {'status': 'error', 'message': str(e)}

async def main():
    processor = TelemetryProcessor(max_workers=8, batch_size=2000, use_multiprocessing=True)
    
    input_files = ['data-1.json', 'data-2.json']
    
    result = await processor.process_enterprise_telemetry(input_files, 'enterprise-telemetry-output.json')
    
    print(f"Processing Result: {json.dumps(result, indent=2)}")

if __name__ == "__main__":
    asyncio.run(main())
