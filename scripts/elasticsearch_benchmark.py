#!/usr/bin/env python3
"""
Elasticsearch Benchmark Script (Complete workflow)
Handles setup, data loading, and benchmarking
"""

import sys
import time
import json
import os
import subprocess
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from concurrent.futures import ThreadPoolExecutor, as_completed
except ImportError:
    # concurrent.futures is built-in in Python 3
    pass

def create_session():
    """Create a requests session with connection pooling"""
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=3,
        status_forcelist=[429, 500, 502, 503, 504],
        backoff_factor=1
    )
    
    # Configure adapter with connection pooling
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def wait_for_elasticsearch(session, es_host, es_port, quiet=False):
    """Wait for Elasticsearch to be ready"""
    if not quiet:
        print("Waiting for Elasticsearch to be ready...")
    
    url = f"http://{es_host}:{es_port}/_cluster/health"
    max_attempts = 30
    attempt = 0
    
    while attempt < max_attempts:
        try:
            response = session.get(url, timeout=5)
            if response.status_code == 200:
                health = response.json()
                if health.get('status') in ['green', 'yellow']:
                    if not quiet:
                        print("Elasticsearch is ready!")
                    return True
        except requests.RequestException:
            pass
        
        if not quiet:
            print(f"Waiting for Elasticsearch... (attempt {attempt + 1}/{max_attempts})")
        time.sleep(2)
        attempt += 1
    
    print("Elasticsearch failed to become ready", file=sys.stderr)
    return False

def setup_index(session, es_host, es_port, index_name, quiet=False):
    """Delete and recreate the index"""
    if not quiet:
        print("Setting up index...")
    
    start_time = time.perf_counter()
    
    # Delete index if exists
    delete_url = f"http://{es_host}:{es_port}/{index_name}"
    try:
        session.delete(delete_url, timeout=10)
    except requests.RequestException:
        pass  # Index might not exist
    
    # Create index with mapping
    create_url = f"http://{es_host}:{es_port}/{index_name}"
    mapping = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "content": {"type": "text"},
                "join_field": { 
                    "type": "join",
                    "relations": {
                        "parent": "child" 
                    }
                }
            }
        }
    }
    
    response = session.put(create_url, json=mapping, timeout=10)
    if response.status_code not in [200, 201]:
        print(f"Failed to create index: {response.text}", file=sys.stderr)
        return False
    
    end_time = time.perf_counter()
    index_creation_time = end_time - start_time
    
    if not quiet:
        print("Index created")
    
    # Save index creation time
    with open('/tmp/index_creation_time.txt', 'w') as f:
        f.write(f"Index creation time: {index_creation_time:.6f}s\n")
    
    return True

def load_data(session, es_host, es_port, index_name, scale, quiet=False):
    """Load data using bulk API"""
    if not quiet:
        print("Loading data...")
    
    start_time = time.perf_counter()
    
    # Load config
    config_file = '/config/benchmark_config.json'
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Get expected size from scale-specific config
    scale_size_map = {
        'small': 'small_scale',
        'medium': 'medium_scale', 
        'large': 'large_scale'
    }
    expected_size = config['data'][scale_size_map[scale]]
    
    data_file = f'/data/documents_{scale}.json'
    
    if not quiet:
        print(f"Loading data from {data_file}...")
    
    # Disable refresh interval for faster loading
    settings_url = f"http://{es_host}:{es_port}/{index_name}/_settings"
    session.put(settings_url, json={"index": {"refresh_interval": "-1"}}, timeout=10)

    # Send bulk requests in batches
    batch_size = 5000
    bulk_data = []
    
    bulk_url = f"http://{es_host}:{es_port}/_bulk?refresh=true"
    headers = {'Content-Type': 'application/x-ndjson'}
    
    def flush_batch(data):
        if not data: return True
        body = "\n".join(data) + "\n"
        response = session.post(bulk_url, data=body, headers=headers, timeout=60)
        if response.status_code not in [200, 201]:
            print(f"Bulk load failed: {response.text}", file=sys.stderr)
            return False
        return True

    # Load parents
    count = 0
    with open(data_file, 'r') as f:
        for line in f:
            try:
                doc = json.loads(line)
                action = {"index": {"_index": index_name, "_id": str(doc['id'])}}
                doc['join_field'] = 'parent'
                
                bulk_data.append(json.dumps(action))
                bulk_data.append(json.dumps(doc))
                count += 1
                
                if len(bulk_data) >= batch_size * 2:
                    if not flush_batch(bulk_data): return False
                    bulk_data = []
                
                if count >= expected_size:
                    break
            except json.JSONDecodeError:
                continue
                
    if bulk_data:
        if not flush_batch(bulk_data): return False
        bulk_data = []
        
    print(f"Loaded {count} parent documents")

    # Load children
    child_data_file = f'/data/documents_child_{scale}.json'
    if os.path.exists(child_data_file):
        print(f"Loading child data from {child_data_file}...")
        child_count = 0
        with open(child_data_file, 'r') as f:
            for line in f:
                try:
                    doc = json.loads(line)
                    action = {"index": {"_index": index_name, "routing": str(doc['parent_id'])}}
                    doc['join_field'] = {'name': 'child', 'parent': str(doc['parent_id'])}
                    
                    bulk_data.append(json.dumps(action))
                    bulk_data.append(json.dumps(doc))
                    child_count += 1
                    
                    if len(bulk_data) >= batch_size * 2:
                        if not flush_batch(bulk_data): return False
                        bulk_data = []
                except json.JSONDecodeError:
                    continue
        
        if bulk_data:
            if not flush_batch(bulk_data): return False
            bulk_data = []
        print(f"Loaded {child_count} child documents")
    
    # Restore refresh interval
    session.put(settings_url, json={"index": {"refresh_interval": "1s"}}, timeout=10)

    # Refresh index
    refresh_url = f"http://{es_host}:{es_port}/{index_name}/_refresh"
    session.post(refresh_url, timeout=10)
    
    # Force merge to optimize index (similar to VACUUM)
    if not quiet:
        print("Force merging index...")
    forcemerge_url = f"http://{es_host}:{es_port}/{index_name}/_forcemerge?max_num_segments=1"
    session.post(forcemerge_url, timeout=300) # Long timeout for merge

    # Wait for all documents to be available
    if not quiet:
        print("Waiting for all documents to be indexed...")
    
    count_url = f"http://{es_host}:{es_port}/{index_name}/_count"
    max_retries = 60
    max_retries_reached = True
    for i in range(max_retries):
        try:
            response = session.get(count_url, timeout=10)
            if response.status_code == 200:
                count = response.json().get('count', 0)
                if count >= expected_size:
                    print(f"All documents indexed: {count}")
                    max_retries_reached = False
                    break
        except:
            pass
        time.sleep(1)
    if max_retries_reached:
        print("Timeout waiting for documents to be indexed!!!", file=sys.stderr)
        print(f"Expected: {expected_size}, Indexed: {count}", file=sys.stderr)
        raise Exception("Data loading timeout")
    
    end_time = time.perf_counter()
    loading_time = end_time - start_time
    
    if not quiet:
        print(f"Loaded {count} parent documents and {child_count} child documents")
    
    # Save data loading time
    with open('/tmp/data_loading_time.txt', 'w') as f:
        f.write(f"Data loading time: {loading_time:.6f}s\n")

    # Measure database size
    stats_url = f"http://{es_host}:{es_port}/_stats/store"
    try:
        response = session.get(stats_url, timeout=10)
        if response.status_code == 200:
            size_bytes = response.json()['_all']['primaries']['store']['size_in_bytes']
            with open('/tmp/database_size.txt', 'w') as f:
                f.write(f"Database size: {size_bytes} bytes\n")
    except Exception as e:
        print(f"Failed to measure database size: {e}", file=sys.stderr)
    
    return True

def count_documents(session, es_host, es_port, index_name, quiet=False):
    """Count documents in index"""
    if not quiet:
        print("Counting documents in index...")
    
    count_url = f"http://{es_host}:{es_port}/{index_name}/_count"
    response = session.get(count_url, timeout=10)
    
    if response.status_code == 200:
        count = response.json().get('count', 0)
        if not quiet:
            print(f"Total documents in index: {count}")
        return count
    else:
        print(f"Failed to count documents: {response.text}", file=sys.stderr)
        return 0

def run_query(session, es_host, es_port, index_name, query_body):
    """Run a single Elasticsearch query"""
    url = f"http://{es_host}:{es_port}/{index_name}/_search"
    
    start_time = time.perf_counter()
    
    try:
        response = session.get(
            url,
            headers={'Content-Type': 'application/json'},
            json=query_body,
            timeout=10
        )
        response.raise_for_status()

        # Force client-side JSON parsing and minimal materialization of results.
        # Without this, the benchmark mostly measures network/HTTP overhead and
        # server time, but not the cost of decoding/handling responses.
        data = response.json()

        # Walk hits + inner_hits and touch selected fields so the client does
        # comparable work to a DB client fetching/decoding rows.
        hits = data.get('hits', {}).get('hits', [])
        materialized = []
        for hit in hits:
            source = hit.get('_source') or {}
            materialized.append((hit.get('_id'), source.get('id'), source.get('title')))

            inner_hits = hit.get('inner_hits') or {}
            for inner in inner_hits.values():
                inner_docs = inner.get('hits', {}).get('hits', [])
                for inner_hit in inner_docs:
                    inner_source = inner_hit.get('_source') or {}
                    materialized.append((
                        inner_hit.get('_id'),
                        inner_source.get('id'),
                        inner_source.get('title'),
                    ))

        # Prevent accidental dead-code elimination / keep behavior explicit.
        _ = len(materialized)
        
        end_time = time.perf_counter()
        return end_time - start_time
        
    except Exception as e:
        print(f"Query failed: {e}", file=sys.stderr)
        end_time = time.perf_counter()
        return end_time - start_time

def run_concurrent_queries(session, es_host, es_port, index_name, query_type, transactions, concurrency, quiet=False):
    """Run queries concurrently with connection pooling"""
    
    # Load config
    config_file = '/config/benchmark_config.json'
    with open(config_file, 'r') as f:
        benchmark_config = json.load(f)
    
    queries_config = benchmark_config['queries']

    # Query configurations
    query_configs = {
        1: {
            'name': 'Simple Search',
            'terms': queries_config['simple']['terms'],
            'query_template': lambda term: {
                "query": {"match": {"content": term}},
                "size": 10,
                "_source": ["id", "title"],
                "sort": [{"_score": "desc"}]
            }
        },
        2: {
            'name': 'Phrase Search', 
            'terms': queries_config['phrase']['terms'],
            'query_template': lambda phrase: {
                "query": {"match_phrase": {"content": phrase}},
                "size": 10,
                "_source": ["id", "title"],
                "sort": [{"_score": "desc"}]
            }
        },
        3: {
            'name': 'Complex Query',
            'term1s': queries_config['complex']['term1s'],
            'term2s': queries_config['complex']['term2s'],
            'query_template': lambda term1, term2: {
                "query": {"bool": {"should": [
                    {"match": {"content": term1}},
                    {"match": {"content": term2}}
                ]}},
                "size": 20,
                "_source": ["id", "title"],
                "sort": [{"_score": "desc"}]
            }
        },
        4: {
            'name': 'Top-N Query',
            'terms': queries_config['top_n']['terms'],
            'n': queries_config['top_n']['n'],
            'query_template': lambda term, n: {
                "query": {"match": {"content": term}},
                "size": n,
                "_source": ["id", "title"],
                "sort": [{"_score": "desc"}]
            }
        },
        5: {
            'name': 'Boolean Query',
            'must_terms': queries_config['boolean']['must_terms'],
            'should_terms': queries_config['boolean']['should_terms'],
            'not_terms': queries_config['boolean']['not_terms'],
            'query_template': lambda must, should, not_term: {
                "query": {"bool": {
                    "must": [{"match": {"content": must}}],
                    "should": [{"match": {"content": should}}],
                    "must_not": [{"match": {"content": not_term}}],
                    "minimum_should_match": 1
                }},
                "size": 10,
                "_source": ["id", "title"],
                "sort": [{"_score": "desc"}]
            }
        },
        6: {
            'name': 'Join Query',
            'terms': queries_config['simple']['terms'],
            'query_template': lambda term: {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"content": term}},
                            {
                                "has_child": {
                                    "type": "child",
                                    "query": {"match_all": {}},
                                    "inner_hits": {}
                                }
                            }
                        ]
                    }
                },
                "size": 10,
                "_source": ["id", "title"],
                "sort": [{"_score": "desc"}]
            }
        }
    }
    
    config = query_configs[query_type]
    if not quiet:
        print(f"Query {query_type}: {config['name']} ({transactions} iterations, concurrency: {concurrency})")
    
    # Calculate transactions per worker
    transactions_per_worker = (transactions + concurrency - 1) // concurrency
    
    completed_transactions = 0
    
    def worker_task(worker_id):
        worker_time = 0
        worker_transactions = 0
        
        start_idx = (worker_id - 1) * transactions_per_worker + 1
        end_idx = min(worker_id * transactions_per_worker, transactions)
        
        for i in range(start_idx, end_idx + 1):
            if query_type == 3:
                term1_idx = (i - 1) % len(config['term1s'])
                term2_idx = (i - 1) % len(config['term2s'])
                term1 = config['term1s'][term1_idx]
                term2 = config['term2s'][term2_idx]
                query_body = config['query_template'](term1, term2)
            elif query_type == 4:
                term_idx = (i - 1) % len(config['terms'])
                term = config['terms'][term_idx]
                query_body = config['query_template'](term, config['n'])
            elif query_type == 5:
                must_idx = (i - 1) % len(config['must_terms'])
                should_idx = (i - 1) % len(config['should_terms'])
                not_idx = (i - 1) % len(config['not_terms'])
                must = config['must_terms'][must_idx]
                should = config['should_terms'][should_idx]
                not_term = config['not_terms'][not_idx]
                query_body = config['query_template'](must, should, not_term)
            else:
                term_idx = (i - 1) % len(config['terms'])
                term = config['terms'][term_idx]
                query_body = config['query_template'](term)
            
            query_time = run_query(session, es_host, es_port, index_name, query_body)
            worker_time += query_time
            worker_transactions += 1
            
        return worker_time, worker_transactions
    
    # Run workers concurrently and measure wall time
    start_time = time.perf_counter()
    total_latency = 0
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker_task, worker_id) for worker_id in range(1, concurrency + 1)]
        
        for future in as_completed(futures):
            worker_time, worker_transactions = future.result()
            completed_transactions += worker_transactions
            total_latency += worker_time
    end_time = time.perf_counter()
    wall_time = end_time - start_time
    
    avg_latency = total_latency / transactions if transactions > 0 else 0
    
    if not quiet:
        print(f"Average Latency for Query {query_type}: {avg_latency:.6f}s")
        print(f"Wall time for Query {query_type}: {wall_time:.6f}s")
        print(f"TPS for Query {query_type}: {transactions / wall_time:.2f}")
    
    return avg_latency, wall_time

def main():
    # Parse arguments
    quiet = '--quiet' in sys.argv or '-q' in sys.argv
    
    # Elasticsearch connection details
    es_host = os.environ.get('ES_HOST', 'localhost')
    es_port = int(os.environ.get('ES_PORT', '9200'))
    index_name = os.environ.get('INDEX_NAME', 'documents')
    scale = os.environ.get('SCALE', 'small')
    transactions = int(os.environ.get('TRANSACTIONS', '10'))
    concurrency = int(os.environ.get('CONCURRENCY', '1'))
    
    # Create session
    session = create_session()
    
    # Wait for Elasticsearch
    if not wait_for_elasticsearch(session, es_host, es_port, quiet):
        sys.exit(1)
    
    # Setup index
    if not setup_index(session, es_host, es_port, index_name, quiet):
        sys.exit(1)
    
    # Load data
    if not load_data(session, es_host, es_port, index_name, scale, quiet):
        sys.exit(1)
    
    # Count documents
    count_documents(session, es_host, es_port, index_name, quiet)
    
    # Warmup
    if not quiet:
        print("Warming up...")
        
    for query_type in [1, 2, 3, 4, 5, 6]:
        run_concurrent_queries(
            session, es_host, es_port, index_name, query_type,
            transactions=10, # Warmup with 10 transactions
            concurrency=concurrency,
            quiet=True
        )
    
    # Run benchmark queries
    if not quiet:
        print("Running benchmark queries...")
    
    results = {
        "database": "elasticsearch",
        "scale": scale,
        "metrics": {}
    }

    # Collect metrics from files
    try:
        with open('/tmp/data_loading_time.txt', 'r') as f:
            results['metrics']['data_loading_time'] = float(f.read().split(':')[1].strip().rstrip('s'))
    except: pass

    try:
        with open('/tmp/index_creation_time.txt', 'r') as f:
            results['metrics']['index_creation_time'] = float(f.read().split(':')[1].strip().rstrip('s'))
    except: pass
    
    try:
        with open('/tmp/database_size.txt', 'r') as f:
            results['metrics']['database_size_bytes'] = int(f.read().split(':')[1].strip().split(' ')[0])
    except: pass

    for query_type in [1, 2, 3, 4, 5, 6]:
        avg_latency, total_time = run_concurrent_queries(
            session, es_host, es_port, index_name, query_type,
            transactions, concurrency, quiet
        )
        
        results['metrics'][f'query_{query_type}'] = {
            "average_latency": avg_latency,
            "total_time": total_time,
            "tps": transactions / total_time if total_time > 0 else 0
        }
        
        # Write results to files (matching the shell script output format)
        with open(f'/tmp/query{query_type}_time.txt', 'w') as f:
            f.write(f"Average Latency for Query {query_type}: {avg_latency:.6f}s\n")
            f.write(f"Wall time for Query {query_type}: {total_time:.6f}s\n")
    
    # Write full results to JSON
    with open('/tmp/results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    if not quiet:
        print("Benchmark completed. Results saved to /tmp/")

if __name__ == "__main__":
    main()