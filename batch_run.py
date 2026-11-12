# This script runs a container repeatedly with the given list of CUSTOM_DATE env vars and the base env var file.

# The data to execute with will be formatted in JSON, like so:
# {
#     "run_data": [
#         {
#             "container": "ghcr.io/hcdp/task-preliminary-rainfall-daily:latest",
#             "envs": {
#                 "files": ["/home/exouser/container_envs/aggregation/aggregation.env"]
#             }
#         },
#         {
#             "container": "ghcr.io/hcdp/task-ingest-values:latest",
#             "envs": {
#                 "variables": {
#                     "INGESTION_CONFIG_URL": "https://raw.githubusercontent.com/hcdp/preliminary_rainfall/main/containers/daily/configs/ingestion.json"
#                 },
#                 "files": ["/home/exouser/container_envs/ingestion/ingestion.env"]
#             }
#         }
#     ],
#     "parallel": {
#       "concurrency": 4
#     },
#     "date_ranges": [
#         "2021-01-01_2024-01-01"
#     ]
# }

# The script will run the container for each date, in series.

import argparse
import json
import docker
from docker.errors import APIError, ContainerError
from datetime import datetime
from dateutil.relativedelta import relativedelta
from time import time_ns
from math import inf
from os import cpu_count
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from threading import Lock
from collections import defaultdict
from functools import partial
import logging
from docker.utils import parse_env_file, parse_repository_tag

logging.basicConfig(level = logging.INFO, format = "%(message)s")

id_lock = Lock()
pull_lock = Lock()
pulled_images: set[str] = set()
docker_client = docker.from_env()

def partition_dates(dates: list[str], partition_by: str, date_format: str = "%Y-%m-%d") -> list[list[str]]:
    partitions = defaultdict(list)
    
    for date_str in dates:
        dt = datetime.strptime(date_str, date_format)
        
        if partition_by == "year":
            key = dt.year
        elif partition_by == "month":
            key = (dt.year, dt.month)
        else:
            key = date_str
            
        partitions[key].append(date_str)
        
    # Return sorted by time keys
    return [partitions[k] for k in sorted(partitions.keys())]


def partition_date_ranges(date_ranges: list[str], partition_by: str, date_format: str = "%Y-%m-%d") -> list[list[tuple[str, str]]]:
    partitions = defaultdict(list)
    
    for date_range in date_ranges:
        start_str, end_str = parse_date_range(date_range)
        start_dt = datetime.strptime(start_str, date_format)
        end_dt = datetime.strptime(end_str, date_format)
        
        current_start = start_dt
        
        while current_start <= end_dt:
            # Determine the natural end boundary for the current partition
            if partition_by == "year":
                key = current_start.year
                # End of the current year
                partition_end = datetime(current_start.year, 12, 31)
            elif partition_by == "month":
                key = (current_start.year, current_start.month)
                # End of the current month
                partition_end = current_start + relativedelta(day=31)
            else:
                key = current_start
                partition_end = end_dt
                
            # The actual end date is either the end of the partition, or the end of the total range (whichever is earlier)
            actual_end = min(partition_end, end_dt)
            
            partitions[key].append(
                (current_start.strftime(date_format), actual_end.strftime(date_format))
            )
            
            # Step forward to the first day of the next partition
            current_start = actual_end + relativedelta(days=1)
            
    # Return sorted by time keys
    return [partitions[k] for k in sorted(partitions.keys())]
    
    
def handle_dates(dates: list[str], run_data: Any, dry_run: bool, container_ids: list[str], max_containers: int):
    for date in dates:
        run_containers(date, run_data, dry_run, container_ids, max_containers)

def handle_date_ranges(date_ranges: list[tuple[str, str]], run_data: Any, delta: Any, date_format: str, dry_run: bool, container_ids: list[str], max_containers: int):
    for start_date, end_date in date_ranges:
        for date in generate_dates(start_date, end_date, delta, date_format):
            run_containers(date, run_data, dry_run, container_ids, max_containers)


def parse_date_range(date_range) -> tuple[str, str]:
    start_date, end_date = date_range.split('_')
    return start_date, end_date

def generate_dates(start_date, end_date, delta, date_format):
    current_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    while current_date <= end_date:
        yield current_date.strftime(date_format)
        current_date += relativedelta(**delta)
        

def run_containers(date, run_data, dry_run, container_ids, max_containers):
    for data in run_data:
        container = str(data["container"])
        envs = data.get("envs", {})
        variable_envs = envs.get("variables", {})
        file_envs = envs.get("files", [])
        mounts = data.get("mounts", [])
        
        # Format Volumes
        volumes = [f"{src}:{dst}" for src, dst in mounts]
        
        # Build Environment Dictionary
        environment = {"CUSTOM_DATE": date}
        if variable_envs:
            environment.update(variable_envs)
        for file in file_envs:
            environment.update(parse_env_file(file))

        # Thread-safe check to see if we've already pulled this container
        with pull_lock:
            needs_pull = container not in pulled_images
            if needs_pull and not dry_run:
                pulled_images.add(container)
        
        if needs_pull and not dry_run:
            logging.info(f'Pulling updates for {container} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
            try:
                repo, tag = parse_repository_tag(container)
                docker_client.images.pull(repo, tag = tag or "latest")
            except APIError as e:
                logging.info(f"Warning: Failed to pull image {container}, will attempt to run with cache if present. Error: {e}")

        
        if not dry_run:
            container_name = f"batch_{date}_{time_ns()}"
            
            run_kwargs: dict[str, Any] = {
                "detach": True,
                "name": container_name,
                "environment": environment,
                "volumes": volumes
            }
            
            try:
                container = docker_client.containers.run(container, **run_kwargs)
                logging.info(f'Running container {container} with CUSTOM_DATE={date} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n{container.id}')
                result = container.wait()
                exit_code = result.get("StatusCode")
                logging.info(f"Container {container.id} finished with exit code {exit_code}")
            except ContainerError as e:
                logging.info(f"Container failed to run: {e}")
            except Exception as e:
                logging.info(f"Unexpected error running {container}: {e}")

            with id_lock:
                container_ids.append(container_name)
                #remove the oldest container if over the max allowed number of containers have been executed
                if len(container_ids) > max_containers:
                    removed_name = container_ids.pop(0)
                    try:
                        c_rm = docker_client.containers.get(removed_name)
                        c_rm.remove()
                    except APIError as e:
                        logging.info(f"Failed to remove container {removed_name}: {e}")
    else:
        logging.info(f'Running container {container} with CUSTOM_DATE={date} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

def main():
    parser = argparse.ArgumentParser(description='Run a container repeatedly with the given list of CUSTOM_DATE env vars.')
    parser.add_argument('data', help='JSON file specifying the containers, env data, and dates to run with')
    parser.add_argument('-d', '--dry-run', action='store_true', help='Do not run the container, just print the commands that would be run')
    args = parser.parse_args()

    with open(args.data) as f:
        data = json.load(f)

    run_data = data['run_data']
    dates = data.get('dates', [])
    date_ranges = data.get('date_ranges', [])
    delta = data.get("delta", {
        "days": 1
    })
    date_format = data.get("date_format", "%Y-%m-%d")
    max_containers = data.get("max_stored", inf)
    
    concurrency = 1
    partition_by = None
    
    parallel = data.get("parallel")
    if parallel is not None:
        partition_by = parallel.get("partition_by", "year")
        concurrency_max = cpu_count() or 1
        concurrency = parallel.get("concurrency", concurrency_max)
        if concurrency < 1 or concurrency > concurrency_max:
            concurrency = concurrency_max
        
    
    partitioned_dates: list[list[str]] = []
    partitioned_date_ranges: list[list[tuple[str, str]]] = []
    if partition_by is not None:
        partitioned_dates = partition_dates(dates, partition_by, date_format)
        partitioned_date_ranges = partition_date_ranges(date_ranges, partition_by, date_format)
    else:
        partitioned_dates = [dates]
        partitioned_date_ranges = [[parse_date_range(date_range) for date_range in date_ranges]]
    

    container_ids: list[str] = []
    
    bound_handle_dates = partial(handle_dates, run_data = run_data, dry_run = args.dry_run, container_ids = container_ids, max_containers = max_containers)
    bound_handle_date_ranges = partial(handle_date_ranges, run_data = run_data, delta = delta, date_format = date_format, dry_run = args.dry_run, container_ids = container_ids, max_containers = max_containers)
    
    with ThreadPoolExecutor(max_workers = concurrency) as executor:
        list(executor.map(bound_handle_dates, partitioned_dates))
        list(executor.map(bound_handle_date_ranges, partitioned_date_ranges))

if __name__ == '__main__':
    main()