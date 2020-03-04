#!/usr/bin/env python3

import facebook_utils

import argparse
import json
import random

START = "start"
FINISH = "finish"
CANCEL = "cancel"
RESTART = "restart"
NEXT = "next"
CREATE = "create"

EXPERIMENT_TYPES = ["us", "uk", "ca", "de", "eu"]

def print_json(label, data):
	print("[{}]".format(label))
	print(json.dumps(data, indent = 2))
	print()

parser = argparse.ArgumentParser()
parser.add_argument("command", choices = [START, FINISH, CANCEL, RESTART, NEXT, CREATE], type = str)
parser.add_argument("value", help = "task_key or number of records", type = int, nargs = "?")
args = parser.parse_args()

queue = facebook_utils.QueueManager(db_folder = "../db/test", verbose = True)

command = args.command

# Database operations
if command == START:
	task_key = args.value
	queue.start_task(task_key)
if command == FINISH:
	task_key = args.value
	queue.finish_task(task_key)
	finish_code = random.randint(0, 100)
	finish_log = {
		"access_token": "{:06d}".format(random.randint(0, 999999))
	}
	queue.amend_task(task_key, finish_code, finish_log)
if command == CANCEL:
	task_key = args.value
	queue.cancel_task(task_key)
if command == RESTART:
	task_key = args.value
	queue.restart_task(task_key)

# Check database status
if command == NEXT:
	task = queue.get_next_active_task()
	print_json("next_active_task", task)

# Insert into database
if command == CREATE:
	rows = min(10, args.value)
	for row in range(0, rows):
		experiment_type = EXPERIMENT_TYPES[random.randint(0, len(EXPERIMENT_TYPES))]
		task_manager = facebook_utils.TaskManager(verbose = True)
		experiment_spec = task_manager.create_experiment(experiment_type)
		split_specs = task_manager.create_splits(experiment_spec)
		page_spec = task_manager.init_page()
		attempt_spec = task_manager.init_attempt()
		continuation = task_manager.init_continuation()
		queue.create_next_tasks(experiment_spec, split_specs, page_spec, attempt_spec, continuation)
