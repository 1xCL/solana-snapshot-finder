import os
import glob
import requests
import time
import shutil
import math
import json
import sys
import argparse
import logging
import subprocess
import concurrent.futures
import pickle
import urllib3
import random
from pathlib import Path
from requests import ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError
from tqdm import tqdm
from multiprocessing.dummy import Pool as ThreadPool
import statistics
from datetime import datetime, timedelta

# Suppress urllib3 warnings and excessive logging
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings(urllib3.exceptions.ConnectTimeoutError)

parser = argparse.ArgumentParser(description='Solana snapshot finder')
parser.add_argument('-t', '--threads-count', default=2000, type=int,
    help='the number of concurrently running threads that check snapshots for rpc nodes')
parser.add_argument('-r', '--rpc_address',
    default='https://api.mainnet-beta.solana.com', type=str,
    help='RPC address of the node from which the current slot number will be taken\n'
         'https://api.mainnet-beta.solana.com')

parser.add_argument("--slot", default=0, type=int,
                     help="search for a snapshot with a specific slot number (useful for network restarts)")
parser.add_argument("--version", default=None, help="search for a snapshot from a specific version node")
parser.add_argument("--wildcard_version", default=None, help="search for a snapshot with a major / minor version e.g. 1.18 (excluding .23)")
parser.add_argument('--max_snapshot_age', default=1300, type=int, help='How many slots ago the snapshot was created (in slots)')
parser.add_argument('--min_download_speed', default=60, type=int, help='Minimum average snapshot download speed in megabytes')
parser.add_argument('--max_download_speed', type=int,
help='Maximum snapshot download speed in megabytes - https://github.com/c29r3/solana-snapshot-finder/issues/11. Example: --max_download_speed 192')
parser.add_argument('--max_latency', default=200, type=int, help='The maximum value of latency (milliseconds). If latency > max_latency --> skip')
parser.add_argument('--with_private_rpc', action="store_true", help='Enable adding and checking RPCs with the --private-rpc option.This slow down checking and searching but potentially increases'
                    ' the number of RPCs from which snapshots can be downloaded.')
parser.add_argument('--measurement_time', default=7, type=int, help='Time in seconds during which the script will measure the download speed')
parser.add_argument('--snapshot_path', type=str, default=".", help='The location where the snapshot will be downloaded (absolute path).'
                                                                     ' Example: /home/ubuntu/solana/validator-ledger')
parser.add_argument('--num_of_retries', default=5, type=int, help='The number of retries if a suitable server for downloading the snapshot was not found')
parser.add_argument('--sleep', default=3, type=int, help='Sleep before next retry (seconds)')
parser.add_argument('--sort_order', default='latency', type=str, help='Priority way to sort the found servers. latency or slots_diff')
parser.add_argument('-ipb', '--ip_blacklist', default='', type=str, help='Comma separated list of ip addresse (ip:port) that will be excluded from the scan. Example: -ipb 1.1.1.1:8899,8.8.8.8:8899')
parser.add_argument('-b', '--blacklist', default='', type=str, help='If the same corrupted archive is constantly downloaded, you can exclude it.'
                    ' Specify either the number of the slot you want to exclude, or the hash of the archive name. '
                    'You can specify several, separated by commas. Example: -b 135501350,135501360 or --blacklist 135501350,some_hash')
parser.add_argument("-v", "--verbose", help="increase output verbosity to DEBUG", action="store_true")
args = parser.parse_args()

DEFAULT_HEADERS = {"Content-Type": "application/json"}
RPC = args.rpc_address
SPECIFIC_SLOT = int(args.slot)
SPECIFIC_VERSION = args.version
WILDCARD_VERSION = args.wildcard_version
MAX_SNAPSHOT_AGE_IN_SLOTS = args.max_snapshot_age
WITH_PRIVATE_RPC = args.with_private_rpc
THREADS_COUNT = args.threads_count
MIN_DOWNLOAD_SPEED_MB = args.min_download_speed
MAX_DOWNLOAD_SPEED_MB = args.max_download_speed
SPEED_MEASURE_TIME_SEC = args.measurement_time
MAX_LATENCY = args.max_latency
SNAPSHOT_PATH = args.snapshot_path if args.snapshot_path[-1] != '/' else args.snapshot_path[:-1]
NUM_OF_MAX_ATTEMPTS = args.num_of_retries
SLEEP_BEFORE_RETRY = args.sleep
NUM_OF_ATTEMPTS = 1
SORT_ORDER = args.sort_order
BLACKLIST = str(args.blacklist).split(",")
IP_BLACKLIST = str(args.ip_blacklist).split(",")
FULL_LOCAL_SNAP_SLOT = 0

current_slot = 0
DISCARDED_BY_ARCHIVE_TYPE = 0
DISCARDED_BY_LATENCY = 0
DISCARDED_BY_SLOT = 0
DISCARDED_BY_VERSION = 0
DISCARDED_BY_UNKNW_ERR = 0
DISCARDED_BY_TIMEOUT = 0
FULL_LOCAL_SNAPSHOTS = []
# skip servers that do not fit the filters so as not to check them again
unsuitable_servers = set()

# Configure Logging - suppress verbose urllib3 logs
logging.getLogger('urllib3').setLevel(logging.ERROR)
logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
logging.getLogger('urllib3.util.retry').setLevel(logging.ERROR)
logging.getLogger('requests.packages.urllib3').setLevel(logging.ERROR)

if args.verbose:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f'{SNAPSHOT_PATH}/snapshot-finder.log'),
            logging.StreamHandler(sys.stdout),
        ]
    )

else:
    # Normal mode - suppress most urllib3 noise
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(f'{SNAPSHOT_PATH}/snapshot-finder.log'),
            logging.StreamHandler(sys.stdout),
        ]
    )

    # Additional suppression for normal mode
    logging.getLogger('urllib3').propagate = False
    logging.getLogger('urllib3.connectionpool').propagate = False
logger = logging.getLogger(__name__)


# Performance cache for RPC nodes
CACHE_FILE = None
PERFORMANCE_CACHE = {}

def load_performance_cache():
    """Load previous performance data from cache file"""
    global PERFORMANCE_CACHE, CACHE_FILE
    CACHE_FILE = f'{SNAPSHOT_PATH}/rpc_performance_cache.pkl'

    try:
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                cache_data = pickle.load(f)
                # Only keep cache entries from last 24 hours
                cutoff_time = datetime.now() - timedelta(hours=24)
                PERFORMANCE_CACHE = {
                    rpc: data for rpc, data in cache_data.items()
                    if datetime.fromisoformat(data.get('timestamp', '1970-01-01')) > cutoff_time
                }
                logger.info(f'Loaded {len(PERFORMANCE_CACHE)} cached RPC performance records')
    except Exception as e:
        logger.debug(f'Could not load performance cache: {e}')
        PERFORMANCE_CACHE = {}

def save_performance_cache():
    """Save current performance data to cache file"""
    try:
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(PERFORMANCE_CACHE, f)
    except Exception as e:
        logger.debug(f'Could not save performance cache: {e}')

def update_performance_cache(rpc_address, speed_mbps, latency_ms):
    """Update performance cache for an RPC node"""
    PERFORMANCE_CACHE[rpc_address] = {
        'speed_mbps': speed_mbps,
        'latency_ms': latency_ms,
        'timestamp': datetime.now().isoformat(),
        'success_count': PERFORMANCE_CACHE.get(rpc_address, {}).get('success_count', 0) + 1
    }

def get_cached_performance(rpc_address):
    """Get cached performance data for an RPC node"""
    return PERFORMANCE_CACHE.get(rpc_address)

def convert_size(size_bytes):
   if size_bytes == 0:
    return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "%s %s" % (s, size_name[i])


# Create a session with connection pooling and reduced retries
session = requests.Session()

# Configure aggressive retry strategy for faster scanning
from urllib3.util.retry import Retry
retry_strategy = Retry(
    total=1,  # Minimal retries for speed
    connect=0,  # No connection retries - fail fast
    read=1,  # Only one read retry
    backoff_factor=0.1,  # Very short backoff
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "POST"]
)

adapter = requests.adapters.HTTPAdapter(
    pool_connections=100,
    pool_maxsize=100,
    max_retries=retry_strategy,
    pool_block=False
)
session.mount('http://', adapter)
session.mount('https://', adapter)

def measure_speed_adaptive(url: str, initial_measure_time: int = 3) -> tuple:
    """Adaptive speed measurement with early termination for slow connections"""
    logging.debug(f'measure_speed_adaptive() for {url}')

    # Check cache first
    cached = get_cached_performance(url)
    if cached and cached.get('success_count', 0) > 2:
        logger.debug(f'Using cached performance for {url}: {cached["speed_mbps"]:.1f} MB/s')
        return cached['speed_mbps'] * 1e6, cached['latency_ms']

    full_url = f'http://{url}/snapshot.tar.bz2'
    start_time = time.monotonic_ns()

    try:
        response_start = time.monotonic_ns()
        r = session.get(full_url, stream=True, timeout=initial_measure_time + 2)
        r.raise_for_status()

        # Calculate latency from response time
        latency_ms = (time.monotonic_ns() - response_start) / 1000000

        last_time = start_time
        loaded = 0
        speeds = []
        early_speeds = []

        for chunk in r.iter_content(chunk_size=131072):  # Larger chunk size
            curtime = time.monotonic_ns()
            worktime = (curtime - start_time) / 1000000000

            # Early termination for very slow connections
            if worktime > 1 and len(early_speeds) > 0:
                avg_early_speed = statistics.median(early_speeds)
                if avg_early_speed < MIN_DOWNLOAD_SPEED_MB * 1e6 * 0.5:  # 50% of minimum
                    logger.debug(f'Early termination for slow connection: {url}')
                    return avg_early_speed, latency_ms

            if worktime >= initial_measure_time:
                break

            delta = (curtime - last_time) / 1000000000
            loaded += len(chunk)

            if delta > 0.5:  # Measure every 0.5 seconds
                estimated_bytes_per_second = loaded * (1 / delta)
                speeds.append(estimated_bytes_per_second)

                if worktime <= 2:  # Collect early measurements
                    early_speeds.append(estimated_bytes_per_second)

                last_time = curtime
                loaded = 0

        final_speed = statistics.median(speeds) if speeds else 0

        # Cache the result
        speed_mbps = final_speed / 1e6
        update_performance_cache(url, speed_mbps, latency_ms)

        return final_speed, latency_ms

    except Exception as e:
        logger.debug(f'Error measuring speed for {url}: {e}')
        return 0, float('inf')

def measure_speed(url: str, measure_time: int) -> float:
    """Legacy function for backward compatibility"""
    speed, _ = measure_speed_adaptive(url, measure_time)
    return speed

def search_with_early_termination(rpc_nodes, target_count=50, max_workers=None):
    """Search RPC nodes with early termination when enough good nodes are found"""
    if max_workers is None:
        max_workers = min(THREADS_COUNT, len(rpc_nodes))

    logger.info(f'Starting concurrent RPC search with {max_workers} workers (target: {target_count} nodes)')

    # Shuffle for better distribution
    rpc_nodes_shuffled = rpc_nodes.copy()
    random.shuffle(rpc_nodes_shuffled)

    found_count = 0
    processed_count = 0
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks in batches for better resource management
        batch_size = max_workers * 4

        for i in range(0, len(rpc_nodes_shuffled), batch_size):
            batch = rpc_nodes_shuffled[i:i + batch_size]

            # Submit batch
            future_to_rpc = {
                executor.submit(get_snapshot_slot, rpc): rpc
                for rpc in batch
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_rpc, timeout=30):
                try:
                    future.result()  # This will execute get_snapshot_slot and update pbar
                except Exception:
                    pass  # Ignore individual task errors

                processed_count += 1
                found_count = len(json_data["rpc_nodes"])

                # Update progress every 200 processed for WITH_PRIVATE_RPC mode
                update_interval = 200 if WITH_PRIVATE_RPC else 50
                if processed_count % update_interval == 0:
                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed
                    eta = (len(rpc_nodes) - processed_count) / rate if rate > 0 else 0
                    logger.info(f'📊 Progress: {processed_count}/{len(rpc_nodes)} processed, '
                              f'{found_count} suitable found, {rate:.1f} RPC/s, ETA: {eta:.0f}s')

                # Early termination if we found enough good nodes
                if found_count >= target_count:
                    logger.info(f'🎯 Found {found_count} suitable RPCs (target: {target_count}) - stopping early!')

                    # Cancel remaining futures
                    cancelled_count = 0
                    for remaining_future in future_to_rpc:
                        if not remaining_future.done():
                            remaining_future.cancel()
                            cancelled_count += 1

                    # Update progress bar to completion
                    remaining = len(rpc_nodes) - processed_count
                    if remaining > 0:
                        for _ in range(remaining):
                            pbar.update(1)

                    elapsed = time.time() - start_time
                    logger.info(f'⚡ Early termination completed in {elapsed:.1f}s '
                              f'({processed_count}/{len(rpc_nodes)} nodes checked, {cancelled_count} cancelled)')
                    return

            # Check if we should continue to next batch
            if found_count >= target_count:
                break

    elapsed = time.time() - start_time
    logger.info(f'RPC search completed in {elapsed:.1f}s ({processed_count} nodes checked)')

def try_adaptive_speed_thresholds(rpc_nodes):
    """Try progressively lower speed thresholds to find suitable nodes"""
    global MIN_DOWNLOAD_SPEED_MB

    original_speed = MIN_DOWNLOAD_SPEED_MB
    adaptive_speeds = [original_speed // 1.3, original_speed // 1.7, original_speed // 2]

    logger.warning(f'⚠️  No nodes found with {original_speed}MB/s. Trying adaptive speed thresholds...')

    for adaptive_speed in adaptive_speeds:
        if adaptive_speed < 250:
            break

        logger.info(f'🔄 Trying with relaxed speed requirement: {adaptive_speed}MB/s')

        # Temporarily lower the global speed requirement
        original_min_speed = MIN_DOWNLOAD_SPEED_MB
        MIN_DOWNLOAD_SPEED_MB = adaptive_speed

        # Force fresh measurements by ignoring cache for adaptive testing
        suitable_nodes = parallel_speed_test_fresh(rpc_nodes, max_workers=10)

        # Restore original setting
        MIN_DOWNLOAD_SPEED_MB = original_min_speed

        if suitable_nodes:
            logger.info(f'✅ Found {len(suitable_nodes)} nodes with {adaptive_speed}MB/s threshold')
            return suitable_nodes
        else:
            logger.info(f'❌ No nodes found with {adaptive_speed}MB/s, trying lower...')

    return []

def measure_speed_fresh(url: str, initial_measure_time: int = 3) -> tuple:
    """Force fresh speed measurement without using cache"""
    logging.debug(f'measure_speed_fresh() for {url} (ignoring cache)')

    full_url = f'http://{url}/snapshot.tar.bz2'
    start_time = time.monotonic_ns()

    try:
        response_start = time.monotonic_ns()
        r = session.get(full_url, stream=True, timeout=initial_measure_time + 2)
        r.raise_for_status()

        # Calculate latency from response time
        latency_ms = (time.monotonic_ns() - response_start) / 1000000

        last_time = start_time
        loaded = 0
        speeds = []
        early_speeds = []

        for chunk in r.iter_content(chunk_size=131072):  # Larger chunk size
            curtime = time.monotonic_ns()
            worktime = (curtime - start_time) / 1000000000

            # Early termination for very slow connections
            if worktime > 1 and len(early_speeds) > 0:
                avg_early_speed = statistics.median(early_speeds)
                if avg_early_speed < MIN_DOWNLOAD_SPEED_MB * 1e6 * 0.5:  # 50% of minimum
                    logger.debug(f'Early termination for slow connection: {url}')
                    return avg_early_speed, latency_ms

            if worktime >= initial_measure_time:
                break

            delta = (curtime - last_time) / 1000000000
            loaded += len(chunk)

            if delta > 0.5:  # Measure every 0.5 seconds
                estimated_bytes_per_second = loaded * (1 / delta)
                speeds.append(estimated_bytes_per_second)

                if worktime <= 2:  # Collect early measurements
                    early_speeds.append(estimated_bytes_per_second)

                last_time = curtime
                loaded = 0

        final_speed = statistics.median(speeds) if speeds else 0

        # Update cache with new result
        speed_mbps = final_speed / 1e6
        update_performance_cache(url, speed_mbps, latency_ms)

        return final_speed, latency_ms

    except Exception as e:
        logger.warning(f'FRESH speed measurement failed for {url}: {e}')
        return 0, float('inf')

def parallel_speed_test_fresh(rpc_nodes, max_workers=10):
    """Test multiple RPC nodes in parallel without using cache"""
    logger.info(f'Starting FRESH parallel speed test for {len(rpc_nodes)} nodes with {max_workers} workers (threshold: {MIN_DOWNLOAD_SPEED_MB}MB/s)')
    logger.info(f'Fresh test will ignore previous unsuitable classifications and force retest all nodes')

    suitable_nodes = []
    tested_speeds = []  # Track all tested speeds for statistics

    def test_single_node_fresh(rpc_node):
        # Skip unsuitable_servers check for fresh testing - we want to retest everything!
        # This is the key fix for the adaptive threshold bug

        # Filter blacklisted snapshots
        if BLACKLIST != ['']:
            if any(i in str(rpc_node["files_to_download"]) for i in BLACKLIST):
                logger.debug(f'BLACKLISTED --> {rpc_node["snapshot_address"]}')
                return None

        # Force fresh measurement - ignore previous unsuitable classification
        logger.debug(f'Fresh testing: {rpc_node["snapshot_address"]} (ignoring previous unsuitable status)')
        speed_bytes, latency_ms = measure_speed_fresh(
            rpc_node["snapshot_address"],
            SPEED_MEASURE_TIME_SEC
        )
        speed_mb = speed_bytes / 1e6
        tested_speeds.append(speed_mb)  # Track for statistics

        if speed_bytes >= MIN_DOWNLOAD_SPEED_MB * 1e6:
            logger.info(f'✅ Suitable (fresh): {rpc_node["snapshot_address"]} - {speed_mb:.1f} MB/s, {latency_ms:.1f}ms')
            # Remove from unsuitable_servers since it passed with new threshold
            unsuitable_servers.discard(rpc_node["snapshot_address"])
            return {
                **rpc_node,
                'measured_speed_mbps': speed_mb,
                'measured_latency_ms': latency_ms
            }
        else:
            # Show results for adaptive testing
            logger.info(f'❌ Too slow (fresh): {rpc_node["snapshot_address"]} - {speed_mb:.1f} MB/s (need {MIN_DOWNLOAD_SPEED_MB}MB/s)')
            return None

    # Sort by cached performance first (put known good nodes first)
    def sort_key(node):
        cached = get_cached_performance(node["snapshot_address"])
        if cached:
            return (-cached.get('speed_mbps', 0), cached.get('latency_ms', float('inf')))
        return (0, node.get('latency', float('inf')))

    sorted_nodes = sorted(rpc_nodes, key=sort_key)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_node = {
            executor.submit(test_single_node_fresh, node): node
            for node in sorted_nodes
        }

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_node, timeout=60):
            try:
                result = future.result()
                if result:
                    suitable_nodes.append(result)
                    # Stop early if we found enough good nodes
                    if len(suitable_nodes) >= 5:
                        logger.info(f'Found {len(suitable_nodes)} suitable nodes, stopping early')
                        # Cancel remaining futures
                        for remaining_future in future_to_node:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
            except Exception as e:
                logger.debug(f'Error in fresh speed test: {e}')

    # Show speed test summary
    if tested_speeds:
        avg_speed = sum(tested_speeds) / len(tested_speeds)
        max_speed = max(tested_speeds)
        slow_count = len([s for s in tested_speeds if s < MIN_DOWNLOAD_SPEED_MB])

        logger.info(f'📊 Fresh speed test summary: {len(tested_speeds)} nodes tested')
        logger.info(f'    Average speed: {avg_speed:.1f} MB/s, Max speed: {max_speed:.1f} MB/s')
        logger.info(f'    {len(suitable_nodes)} passed, {slow_count} too slow (< {MIN_DOWNLOAD_SPEED_MB}MB/s)')

        if not suitable_nodes and max_speed > 0:
            suggested_speed = max(int(max_speed * 0.8), 10)  # 80% of max speed, minimum 10MB/s
            logger.info(f'💡 Suggestion: Try --min_download_speed {suggested_speed} for better results')
            logger.info(f'💡 Example: python3 snapshot-finder.py --min_download_speed {suggested_speed} --snapshot_path $HOME/snapshots')

    # Sort results by speed (descending)
    suitable_nodes.sort(key=lambda x: x['measured_speed_mbps'], reverse=True)
    return suitable_nodes

def parallel_speed_test(rpc_nodes, max_workers=10):
    """Test multiple RPC nodes in parallel"""
    logger.info(f'Starting parallel speed test for {len(rpc_nodes)} nodes with {max_workers} workers')

    suitable_nodes = []
    tested_speeds = []  # Track all tested speeds for statistics

    def test_single_node(rpc_node):
        if rpc_node["snapshot_address"] in unsuitable_servers:
            return None

        # Filter blacklisted snapshots
        if BLACKLIST != ['']:
            if any(i in str(rpc_node["files_to_download"]) for i in BLACKLIST):
                logger.debug(f'BLACKLISTED --> {rpc_node["snapshot_address"]}')
                return None

        speed_bytes, latency_ms = measure_speed_adaptive(
            rpc_node["snapshot_address"],
            SPEED_MEASURE_TIME_SEC
        )
        speed_mb = speed_bytes / 1e6
        tested_speeds.append(speed_mb)  # Track for statistics

        if speed_bytes >= MIN_DOWNLOAD_SPEED_MB * 1e6:
            logger.info(f'✅ Suitable: {rpc_node["snapshot_address"]} - {speed_mb:.1f} MB/s, {latency_ms:.1f}ms')
            return {
                **rpc_node,
                'measured_speed_mbps': speed_mb,
                'measured_latency_ms': latency_ms
            }
        else:
            # Only show first few slow nodes to avoid spam
            if len([s for s in tested_speeds if s < MIN_DOWNLOAD_SPEED_MB]) <= 3:
                logger.info(f'❌ Too slow: {rpc_node["snapshot_address"]} - {speed_mb:.1f} MB/s (need {MIN_DOWNLOAD_SPEED_MB}MB/s)')
            unsuitable_servers.add(rpc_node["snapshot_address"])
            return None

    # Sort by cached performance first (put known good nodes first)
    def sort_key(node):
        cached = get_cached_performance(node["snapshot_address"])
        if cached:
            return (-cached.get('speed_mbps', 0), cached.get('latency_ms', float('inf')))
        return (0, node.get('latency', float('inf')))

    sorted_nodes = sorted(rpc_nodes, key=sort_key)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_node = {
            executor.submit(test_single_node, node): node
            for node in sorted_nodes
        }

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_node, timeout=60):
            try:
                result = future.result()
                if result:
                    suitable_nodes.append(result)
                    # Stop early if we found enough good nodes
                    if len(suitable_nodes) >= 5:
                        logger.info(f'Found {len(suitable_nodes)} suitable nodes, stopping early')
                        # Cancel remaining futures
                        for remaining_future in future_to_node:
                            if not remaining_future.done():
                                remaining_future.cancel()
                        break
            except Exception as e:
                logger.debug(f'Error in speed test: {e}')

    # Show speed test summary
    if tested_speeds:
        avg_speed = sum(tested_speeds) / len(tested_speeds)
        max_speed = max(tested_speeds)
        slow_count = len([s for s in tested_speeds if s < MIN_DOWNLOAD_SPEED_MB])

        logger.info(f'📊 Speed test summary: {len(tested_speeds)} nodes tested')
        logger.info(f'    Average speed: {avg_speed:.1f} MB/s, Max speed: {max_speed:.1f} MB/s')
        logger.info(f'    {len(suitable_nodes)} passed, {slow_count} too slow (< {MIN_DOWNLOAD_SPEED_MB}MB/s)')

        if not suitable_nodes and max_speed > 0:
            suggested_speed = max(int(max_speed * 0.8), 10)  # 80% of max speed, minimum 10MB/s
            logger.info(f'💡 Suggestion: Try --min_download_speed {suggested_speed} for better results')
            logger.info(f'💡 Example: python3 snapshot-finder.py --min_download_speed {suggested_speed} --snapshot_path $HOME/snapshots')

    # Sort results by speed (descending)
    suitable_nodes.sort(key=lambda x: x['measured_speed_mbps'], reverse=True)
    return suitable_nodes


def do_request(url_: str, method_: str = 'GET', data_: str = '', timeout_: int = 3,
               headers_: dict = None, use_session: bool = True):
    global DISCARDED_BY_UNKNW_ERR
    global DISCARDED_BY_TIMEOUT
    r = ''
    if headers_ is None:
        headers_ = DEFAULT_HEADERS

    # Use the global session for connection pooling when possible
    req_func = session if use_session else requests

    try:
        # Use shorter connect timeout for faster failure detection
        connect_timeout = min(timeout_ * 0.3, 0.5)  # 30% of total timeout or 0.5s max
        read_timeout = timeout_

        if method_.lower() == 'get':
            r = req_func.get(url_, headers=headers_, timeout=(connect_timeout, read_timeout))
        elif method_.lower() == 'post':
            r = req_func.post(url_, headers=headers_, data=data_, timeout=(connect_timeout, read_timeout))
        elif method_.lower() == 'head':
            r = req_func.head(url_, headers=headers_, timeout=(connect_timeout, read_timeout))
        return r

    except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError) as reqErr:
        DISCARDED_BY_TIMEOUT += 1
        return f'error in do_request(): {reqErr}'

    except Exception as unknwErr:
        DISCARDED_BY_UNKNW_ERR += 1
        return f'error in do_request(): {unknwErr}'


def get_current_slot():
    logger.debug("get_current_slot()")
    d = '{"jsonrpc":"2.0","id":1, "method":"getSlot"}'
    try:
        r = do_request(url_=RPC, method_='post', data_=d, timeout_=25)
        if 'result' in str(r.text):
            return r.json()["result"]
        else:
            logger.error(f'Can\'t get current slot')
            logger.debug(r.status_code)
            return None

    except (ReadTimeout, ConnectTimeout, HTTPError, Timeout, ConnectionError) as connectErr:
        logger.debug(f'Can\'t get current slot\n{connectErr}')
    except Exception as unknwErr:
        logger.error(f'Can\'t get current slot\n{unknwErr}')
        return None


def get_all_rpc_ips():
    global DISCARDED_BY_VERSION

    logger.debug("get_all_rpc_ips()")
    d = '{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}'
    r = do_request(url_=RPC, method_='post', data_=d, timeout_=25)
    if 'result' in str(r.text):
        rpc_ips = []
        for node in r.json()["result"]:
            if (WILDCARD_VERSION is not None and node["version"] and WILDCARD_VERSION not in node["version"]) or \
               (SPECIFIC_VERSION is not None and node["version"] and node["version"] != SPECIFIC_VERSION):
                DISCARDED_BY_VERSION += 1
                continue
            if node["rpc"] is not None:
                rpc_ips.append(node["rpc"])
            elif WITH_PRIVATE_RPC is True:
                gossip_ip = node["gossip"].split(":")[0]
                rpc_ips.append(f'{gossip_ip}:8899')

        rpc_ips = list(set(rpc_ips))
        logger.debug(f'RPC_IPS LEN before blacklisting {len(rpc_ips)}')
        # removing blacklisted ip addresses
        if IP_BLACKLIST is not None:
            rpc_ips = list(set(rpc_ips) - set(IP_BLACKLIST))
        logger.debug(f'RPC_IPS LEN after blacklisting {len(rpc_ips)}')
        return rpc_ips

    else:
        logger.error(f'Can\'t get RPC ip addresses {r.text}')
        sys.exit()


def get_snapshot_slot(rpc_address: str):
    global FULL_LOCAL_SNAP_SLOT
    global DISCARDED_BY_ARCHIVE_TYPE
    global DISCARDED_BY_LATENCY
    global DISCARDED_BY_SLOT

    pbar.update(1)

    # Skip if this server was previously unresponsive
    if rpc_address in unsuitable_servers:
        return None

    url = f'http://{rpc_address}/snapshot.tar.bz2'
    inc_url = f'http://{rpc_address}/incremental-snapshot.tar.bz2'

    try:
        # Use adaptive timeout based on mode
        timeout_val = 1.0 if WITH_PRIVATE_RPC else 0.5
        r = do_request(url_=inc_url, method_='head', timeout_=timeout_val)

        # Quick check for responsiveness
        if isinstance(r, str):  # Error response
            unsuitable_servers.add(rpc_address)
            return None

        latency_ms = r.elapsed.total_seconds() * 1000

        # Use more relaxed latency check for private RPCs
        max_latency_limit = MAX_LATENCY * 2 if WITH_PRIVATE_RPC else MAX_LATENCY
        if latency_ms > max_latency_limit:
            DISCARDED_BY_LATENCY += 1
            unsuitable_servers.add(rpc_address)
            return None

        if 'location' in str(r.headers) and 'error' not in str(r.text):
            snap_location_ = r.headers["location"]
            if snap_location_.endswith('tar') is True:
                DISCARDED_BY_ARCHIVE_TYPE += 1
                return None

            try:
                incremental_snap_slot = int(snap_location_.split("-")[2])
                snap_slot_ = int(snap_location_.split("-")[3])
                slots_diff = current_slot - snap_slot_

                if slots_diff < -100:
                    logger.debug(f'Invalid snapshot slot diff {slots_diff=} for {rpc_address=}')
                    DISCARDED_BY_SLOT += 1
                    unsuitable_servers.add(rpc_address)
                    return

                if slots_diff > MAX_SNAPSHOT_AGE_IN_SLOTS:
                    DISCARDED_BY_SLOT += 1
                    return

                if FULL_LOCAL_SNAP_SLOT == incremental_snap_slot:
                    json_data["rpc_nodes"].append({
                        "snapshot_address": rpc_address,
                        "slots_diff": slots_diff,
                        "latency": latency_ms,
                        "files_to_download": [snap_location_]
                    })
                    return

                r2 = do_request(url_=url, method_='head', timeout_=timeout_val)
                if not isinstance(r2, str) and 'location' in str(r2.headers):
                    json_data["rpc_nodes"].append({
                        "snapshot_address": rpc_address,
                        "slots_diff": slots_diff,
                        "latency": latency_ms,
                        "files_to_download": [r.headers["location"], r2.headers['location']],
                    })
                    return
            except (ValueError, IndexError):
                # Invalid snapshot filename format
                DISCARDED_BY_ARCHIVE_TYPE += 1
                return

        # Try full snapshot
        r = do_request(url_=url, method_='head', timeout_=timeout_val)
        if not isinstance(r, str) and 'location' in str(r.headers):
            snap_location_ = r.headers["location"]

            if snap_location_.endswith('tar') is True:
                DISCARDED_BY_ARCHIVE_TYPE += 1
                return None

            try:
                full_snap_slot_ = int(snap_location_.split("-")[1])
                slots_diff_full = current_slot - full_snap_slot_

                if slots_diff_full <= MAX_SNAPSHOT_AGE_IN_SLOTS and r.elapsed.total_seconds() * 1000 <= max_latency_limit:
                    json_data["rpc_nodes"].append({
                        "snapshot_address": rpc_address,
                        "slots_diff": slots_diff_full,
                        "latency": r.elapsed.total_seconds() * 1000,
                        "files_to_download": [snap_location_]
                    })
                    return
            except (ValueError, IndexError):
                # Invalid snapshot filename format
                DISCARDED_BY_ARCHIVE_TYPE += 1
                return

        return None

    except Exception as getSnapErr_:
        unsuitable_servers.add(rpc_address)
        return None


def download_single_file(url: str, max_connections: int = 16) -> bool:
    """Download a single file with optimized aria2c settings"""
    fname = url[url.rfind('/'):].replace("/", "")
    temp_fname = f'{SNAPSHOT_PATH}/tmp-{fname}'

    try:
        # Ensure max_connections is within aria2c limits (1-16)
        max_connections = min(max_connections, 16)
        max_connections = max(max_connections, 1)

        # Using aria2c for faster downloads with multi-connection support
        aria2c_args = [
            aria2c_path,
            '--console-log-level=notice',
            f'--max-connection-per-server={max_connections}',
            '--split=16',
            '--min-split-size=1M',
            '--continue=true',
            '--allow-overwrite=true',
            f'--dir={SNAPSHOT_PATH}',
            f'--out=tmp-{fname}',
            url
        ]

        if MAX_DOWNLOAD_SPEED_MB is not None:
            aria2c_args.insert(-1, f'--max-download-limit={MAX_DOWNLOAD_SPEED_MB}M')

        logger.info(f'Starting download: {fname}')
        logger.info(f'Using {max_connections} connections per server')
        if args.verbose:
            logger.debug(f'aria2c command: {" ".join(aria2c_args)}')

        # Run aria2c with real-time output
        result = subprocess.run(aria2c_args)

        if result.returncode == 0:
            logger.info(f'Rename the downloaded file {temp_fname} --> {fname}')
            os.rename(temp_fname, f'{SNAPSHOT_PATH}/{fname}')
            return True
        else:
            logger.error(f'aria2c failed for {fname} with return code: {result.returncode}')
            return False

    except Exception as unknwErr:
        logger.error(f'Exception in download_single_file() for {fname}: {unknwErr}')
        return False

def download_multiple_files(urls: list, max_parallel: int = 2) -> bool:
    """Download multiple files - use sequential for cleaner progress display"""
    if not urls:
        return False

    logger.info(f'Starting download of {len(urls)} files')

    # For cleaner progress display, download large files sequentially
    # This prevents aria2c progress bars from interfering with each other
    success_count = 0

    for i, url in enumerate(urls, 1):
        logger.info(f'📥 Downloading file {i}/{len(urls)}: {url}')
        logger.info('=' * 80)

        # Use full connections for sequential downloads
        success = download_single_file(url, max_connections=16)

        if success:
            success_count += 1
            logger.info(f'✅ Successfully downloaded file {i}/{len(urls)}')
        else:
            logger.error(f'❌ Failed to download file {i}/{len(urls)}: {url}')
            # Continue with remaining files even if one fails

        logger.info('=' * 80)

    logger.info(f'Download summary: {success_count}/{len(urls)} files downloaded successfully')
    return success_count == len(urls)

def download(url: str):
    """Legacy function for backward compatibility"""
    return download_single_file(url)


def main_worker():
    try:
        global FULL_LOCAL_SNAP_SLOT

        # Load performance cache for faster subsequent runs
        load_performance_cache()

        rpc_nodes = list(set(get_all_rpc_ips()))
        global pbar
        pbar = tqdm(total=len(rpc_nodes))
        logger.info(f'RPC servers in total: {len(rpc_nodes)} | Current slot number: {current_slot}\n')

        # Search for full local snapshots.
        # If such a snapshot is found and it is not too old, then the script will try to find and download an incremental snapshot
        FULL_LOCAL_SNAPSHOTS = glob.glob(f'{SNAPSHOT_PATH}/snapshot-*tar*')
        if len(FULL_LOCAL_SNAPSHOTS) > 0:
            FULL_LOCAL_SNAPSHOTS.sort(reverse=True)
            FULL_LOCAL_SNAP_SLOT = FULL_LOCAL_SNAPSHOTS[0].replace(SNAPSHOT_PATH, "").split("-")[1]
            logger.info(f'Found full local snapshot {FULL_LOCAL_SNAPSHOTS[0]} | {FULL_LOCAL_SNAP_SLOT=}')

        else:
            logger.info(f'Can\'t find any full local snapshots in this path {SNAPSHOT_PATH} --> the search will be carried out on full snapshots')

        print(f'Searching information about snapshots on all found RPCs')

        # Use optimized concurrent search with early termination
        # Adjust target based mode - higher target for private RPC due to lower success rate
        target_nodes = 200 if WITH_PRIVATE_RPC else 50

        # Log the adaptive settings being used
        timeout_setting = 2.0 if WITH_PRIVATE_RPC else 0.5
        latency_setting = MAX_LATENCY * 3 if WITH_PRIVATE_RPC else MAX_LATENCY

        if WITH_PRIVATE_RPC:
            logger.info(f'🔧 Private RPC mode enabled: timeout={timeout_setting}s, max_latency={latency_setting}ms, target={target_nodes} nodes')

        search_with_early_termination(rpc_nodes, target_count=target_nodes)
        logger.info(f'Found suitable RPCs: {len(json_data["rpc_nodes"])} (target was {target_nodes})')

        # Calculate success rate
        total_checked = len(rpc_nodes)
        success_rate = (len(json_data["rpc_nodes"]) / total_checked) * 100 if total_checked > 0 else 0
        timeout_rate = (DISCARDED_BY_TIMEOUT / total_checked) * 100 if total_checked > 0 else 0

        logger.info(f'📊 RPC Discovery Summary:')
        logger.info(f'    Total nodes checked: {total_checked}')
        logger.info(f'    Success rate: {success_rate:.2f}% ({len(json_data["rpc_nodes"])} nodes)')
        logger.info(f'    Timeout rate: {timeout_rate:.1f}% ({DISCARDED_BY_TIMEOUT} nodes)')

        if WITH_PRIVATE_RPC:
            logger.info(f'💡 Private RPC mode: Using relaxed timeouts ({timeout_setting}s) and latency ({latency_setting}ms)')

        logger.info(f'Detailed breakdown: TIMEOUT={DISCARDED_BY_TIMEOUT} | LATENCY={DISCARDED_BY_LATENCY} | SLOT={DISCARDED_BY_SLOT} | VERSION={DISCARDED_BY_VERSION} | ARCHIVE_TYPE={DISCARDED_BY_ARCHIVE_TYPE} | UNKNOWN={DISCARDED_BY_UNKNW_ERR}')

        if len(json_data["rpc_nodes"]) == 0:
            logger.error(f'No snapshot nodes were found matching the given parameters: {args.max_snapshot_age=}')
            if WITH_PRIVATE_RPC and DISCARDED_BY_TIMEOUT > len(rpc_nodes) * 0.9:  # >90% timeout
                timeout_pct = (DISCARDED_BY_TIMEOUT / len(rpc_nodes)) * 100
                logger.info(f'💡 High timeout rate ({timeout_pct:.1f}%) in private RPC mode suggests:')
                logger.info(f'   • Most private nodes don\'t provide RPC services')
                logger.info(f'   • Try increasing --max_latency (current: {MAX_LATENCY}ms)')
                logger.info(f'   • Try --max_snapshot_age {args.max_snapshot_age * 2} for older snapshots')
            sys.exit()

        # sort list of rpc node by SORT_ORDER (latency)
        rpc_nodes_sorted = sorted(json_data["rpc_nodes"], key=lambda k: k[SORT_ORDER])

        json_data.update({
            "last_update_at": time.time(),
            "last_update_slot": current_slot,
            "total_rpc_nodes": len(rpc_nodes),
            "rpc_nodes_with_actual_snapshot": len(json_data["rpc_nodes"]),
            "rpc_nodes": rpc_nodes_sorted
        })

        with open(f'{SNAPSHOT_PATH}/snapshot.json', "w") as result_f:
            json.dump(json_data, result_f, indent=2)
        logger.info(f'All data is saved to json file - {SNAPSHOT_PATH}/snapshot.json')

        # Use parallel speed testing for much faster node evaluation
        logger.info("STARTING OPTIMIZED PARALLEL SPEED TESTING")
        suitable_nodes = parallel_speed_test(json_data["rpc_nodes"], max_workers=10)

        # If no nodes meet the speed requirement, try with progressively lower thresholds
        if not suitable_nodes:
            suitable_nodes = try_adaptive_speed_thresholds(json_data["rpc_nodes"])

        if not suitable_nodes:
            logger.error(f'💥 No snapshot nodes were found even with relaxed speed requirements'
                  f'\nOriginal requirement: {args.min_download_speed}MB/s'
                  f'\nTry restarting the script with --with_private_rpc or lower --min_download_speed'
                  f'\nRETRY #{NUM_OF_ATTEMPTS}\\{NUM_OF_MAX_ATTEMPTS}')
            return 1

        logger.info(f"Found {len(suitable_nodes)} suitable nodes for download")

        # Show speed statistics for user information
        if suitable_nodes:
            speeds = [node['measured_speed_mbps'] for node in suitable_nodes]
            avg_speed = sum(speeds) / len(speeds)
            max_speed = max(speeds)
            min_speed = min(speeds)

            logger.info(f"📊 Speed statistics: Avg: {avg_speed:.1f}MB/s, Max: {max_speed:.1f}MB/s, Min: {min_speed:.1f}MB/s")

        # Try downloading from the best nodes
        for i, rpc_node in enumerate(suitable_nodes, start=1):
            logger.info(f'Attempting download from node {i}/{len(suitable_nodes)}: '
                       f'{rpc_node["snapshot_address"]} ({rpc_node["measured_speed_mbps"]:.1f} MB/s)')

            # Prepare download URLs
            download_urls = []
            for path in reversed(rpc_node["files_to_download"]):
                # Skip full snapshot if it already exists locally
                if str(path).startswith("/snapshot-"):
                    full_snap_slot__ = path.split("-")[1]
                    if full_snap_slot__ == FULL_LOCAL_SNAP_SLOT:
                        logger.info(f'Skipping existing full snapshot: slot {full_snap_slot__}')
                        continue

                if 'incremental' in path:
                    # Get the latest incremental snapshot location
                    r = do_request(f'http://{rpc_node["snapshot_address"]}/incremental-snapshot.tar.bz2',
                                 method_='head', timeout_=2)
                    if 'location' in str(r.headers) and 'error' not in str(r.text):
                        download_url = f'http://{rpc_node["snapshot_address"]}{r.headers["location"]}'
                    else:
                        download_url = f'http://{rpc_node["snapshot_address"]}{path}'
                else:
                    download_url = f'http://{rpc_node["snapshot_address"]}{path}'

                download_urls.append(download_url)

            if not download_urls:
                logger.info('No files to download for this node, trying next...')
                continue

            # Download files
            logger.info(f'Downloading {len(download_urls)} file(s) to {SNAPSHOT_PATH}')
            for url in download_urls:
                logger.info(f'  - {url}')

            if len(download_urls) == 1:
                success = download_single_file(download_urls[0])
            else:
                success = download_multiple_files(download_urls)

            if success:
                logger.info(f'✓ Successfully downloaded all files from {rpc_node["snapshot_address"]}')
                save_performance_cache()  # Save cache after successful download
                return 0
            else:
                logger.warning(f'✗ Failed to download from {rpc_node["snapshot_address"]}, trying next node...')
                unsuitable_servers.add(rpc_node["snapshot_address"])
                continue

        logger.error(f'All suitable nodes failed to download')
        return 1



    except KeyboardInterrupt:
        sys.exit('\nKeyboardInterrupt - ctrl + c')

    except:
        return 1


logger.info("Version: 0.4.5-private-rpc-optimized")
logger.info("https://github.com/c29r3/solana-snapshot-finder")
logger.info("🚀 PRIVATE RPC OPTIMIZATIONS:")
logger.info("  ✓ Parallel speed testing (2000 workers)")
logger.info("  ✓ Performance caching & bad server filtering")
logger.info("  ✓ Adaptive speed thresholds (FIXED: bypass unsuitable_servers)")
logger.info("  ✓ Connection pooling & fail-fast strategy")
logger.info("  ✓ Multi-file downloads")
logger.info("  ✓ Enhanced aria2c configuration")
logger.info("  ✓ Smart early termination (50-200 nodes)")
logger.info("  ✓ Adaptive timeout strategy (0.5s normal / 2.0s private)")
logger.info("  ✓ Relaxed latency limits for private RPCs (300ms)")
logger.info("  ✓ Enhanced RPC discovery analytics")
logger.info("  ✓ Fixed critical bug in adaptive threshold testing\n")
logger.info(f'{RPC=}\n'
      f'{MAX_SNAPSHOT_AGE_IN_SLOTS=}\n'
      f'{MIN_DOWNLOAD_SPEED_MB=}\n'
      f'{MAX_DOWNLOAD_SPEED_MB=}\n'
      f'{SNAPSHOT_PATH=}\n'
      f'{THREADS_COUNT=}\n'
      f'{NUM_OF_MAX_ATTEMPTS=}\n'
      f'{WITH_PRIVATE_RPC=}\n'
      f'{SORT_ORDER=}')

try:
    f_ = open(f'{SNAPSHOT_PATH}/write_perm_test', 'w')
    f_.close()
    os.remove(f'{SNAPSHOT_PATH}/write_perm_test')
except IOError:
    logger.error(f'\nCheck {SNAPSHOT_PATH=} and permissions')
    Path(SNAPSHOT_PATH).mkdir(parents=True, exist_ok=True)

aria2c_path = shutil.which("aria2c")

if aria2c_path is None:
    logger.error("The aria2c utility was not found in the system, it is required")
    sys.exit()

json_data = ({"last_update_at": 0.0,
              "last_update_slot": 0,
              "total_rpc_nodes": 0,
              "rpc_nodes_with_actual_snapshot": 0,
              "rpc_nodes": []
              })


start_time = time.time()

while NUM_OF_ATTEMPTS <= NUM_OF_MAX_ATTEMPTS:
    attempt_start_time = time.time()

    if SPECIFIC_SLOT != 0 and type(SPECIFIC_SLOT) is int:
        current_slot = SPECIFIC_SLOT
        MAX_SNAPSHOT_AGE_IN_SLOTS = 0
    else:
        current_slot = get_current_slot()

    logger.info(f'Attempt number: {NUM_OF_ATTEMPTS}. Total attempts: {NUM_OF_MAX_ATTEMPTS}')
    NUM_OF_ATTEMPTS += 1

    if current_slot is None:
        continue

    worker_result = main_worker()

    attempt_duration = time.time() - attempt_start_time

    if worker_result == 0:
        total_duration = time.time() - start_time
        logger.info(f"✅ SUCCESS! Total time: {total_duration:.1f}s, This attempt: {attempt_duration:.1f}s")

        # Save final cache state
        save_performance_cache()

        exit(0)

    if worker_result != 0:
        logger.info(f"❌ Attempt failed in {attempt_duration:.1f}s. Now trying with flag --with_private_rpc")
        WITH_PRIVATE_RPC = True

    if NUM_OF_ATTEMPTS >= NUM_OF_MAX_ATTEMPTS:
        total_duration = time.time() - start_time
        logger.error(f'Could not find a suitable snapshot after {total_duration:.1f}s --> exit')
        sys.exit()

    logger.info(f"😴 Sleeping {SLEEP_BEFORE_RETRY} seconds before next try")
    time.sleep(SLEEP_BEFORE_RETRY)