# This script runs a container repeatedly with the given list of CUSTOM_DATE env vars and the base env var file.

# The data to execute with will be formatted in JSON, like so:
# {
#   "containers": []"ghcr.io/hcdp/preliminary-rainfall-monthly:latest"],
#   "env": "/path/to/env_file.env",
#   "dates": [
#     "2023-01-01",
#     "2023-01-02",
#     "2023-01-03"
#   ],
#   "date_ranges": [
#     "2023-01-01_2023-01-05",
#     "2023-01-06_2023-01-10"
#   ]
# }

# The script will run the container for each date, in series.

import argparse
import json
import subprocess
from datetime import datetime, timedelta
from time import time_ns
from math import inf

def parse_date_range(date_range):
    start_date, end_date = date_range.split('_')
    return start_date, end_date

def generate_dates(start_date, end_date, delta, date_format):
    current_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    while current_date <= end_date:
        yield current_date.strftime(date_format)
        current_date += timedelta(**delta)

def run_containers(date, containers, env, dry_run, container_ids, max_containers):
    for container in containers:
        print(f'Running container {container} with CUSTOM_DATE={date} and base env file {base_env} at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        if not args.dry_run:
            container_name = f"batch_{date}_{time_ns()}"
            subprocess.run(['docker', 'run', '-d', f'--name={container_name}', f'--env-file={env}', '-e', f'CUSTOM_DATE={date}', container], check=True, stderr=subprocess.STDOUT)
            subprocess.run(['docker', 'wait', container_name], check=True, stderr=subprocess.STDOUT)

            container_ids.append(container_name)
            #remove the oldest container if over the max allowed number of containers have been executed
            if(len(container_ids) > max_containers):
                removed_name = container_ids.pop(0)
                subprocess.run(['docker', 'rm', removed_name], check=True, stderr=subprocess.STDOUT)

def main():
    parser = argparse.ArgumentParser(description='Run a container repeatedly with the given list of CUSTOM_DATE env vars.')
    parser.add_argument('data', help='JSON file specifying the container, base env file, and dates to run with')
    parser.add_argument('-d', '--dry-run', action='store_true', help='Do not run the container, just print the commands that would be run')
    args = parser.parse_args()

    with open(args.data) as f:
        data = json.load(f)

    containers = data['containers']
    env = data['env']
    dates = data.get('dates', [])
    date_ranges = data.get('date_ranges', [])
    delta = data.get("delta", {
        "days": 1
    })
    date_format = data.get("date_format", "%Y-%m-%d")
    max_containers = data.get("max_stored", inf)

    container_ids = []

    for date in dates:
        run_containers(date, containers, env, args.dry_run, container_ids, max_containers)

    for date_range in date_ranges:
        start_date, end_date = parse_date_range(date_range)
        for date in generate_dates(start_date, end_date, delta, date_format):
            run_containers(date, containers, env, args.dry_run, container_ids, max_containers)

if __name__ == '__main__':
    main()
