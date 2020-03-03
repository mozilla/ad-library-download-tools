#!/usr/bin/env python3

import facebook_utils

import json
import random

def _print_json(label, data):
	print("[{}]".format(label))
	print(json.dumps(data, indent = 2))
	print()

queue_manager = facebook_utils.QueueManager(db_folder = "../db/test", verbose = True)

for experiment_type in ["us", "uk", "de"]:
	task_manager = facebook_utils.TaskManager(verbose = True)
	experiment_spec = task_manager.create_experiment(experiment_type)
	split_specs = task_manager.create_splits(experiment_spec)
	page_spec = task_manager.init_page()
	attempt_spec = task_manager.init_attempt()
	continuation = task_manager.init_continuation()
	queue_manager.create_next_tasks(experiment_spec, split_specs, page_spec, attempt_spec, continuation)

next_task = queue_manager.get_next_task()
_print_json("next_task", next_task)

task_key = next_task["task_key"]
queue_manager.before_task()
queue_manager.start_task(task_key)
queue_manager.finish_task(task_key)

finish_code = random.randint(0, 100)
finish_log = {
	"access_token": "{:06d}".format(random.randint(0, 999999))
}
queue_manager.amend_task(task_key, finish_code, finish_log)
queue_manager.after_task()
