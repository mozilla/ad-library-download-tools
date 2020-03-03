#!/usr/bin/env python3

import facebook_utils

import json
import random

def _print_json(label, data):
	print("[{}]".format(label))
	print(json.dumps(data, indent = 2))
	print()

queue_db = facebook_utils.QueueDB(db_folder = "../db/test", verbose = True)

queue_db.open()
for experiment_type in ["us", "uk", "de"]:
	task_manager = facebook_utils.TaskManager(verbose = True)
	experiment_spec = task_manager.create_experiment(experiment_type)
	split_specs = task_manager.create_splits(experiment_spec)
	for split_spec in split_specs:
		page_spec = task_manager.init_page()
		attempt_spec = task_manager.init_attempt()
		continuation = task_manager.init_continuation()
		queue_db.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
queue_db.close()

queue_db.open()
next_task = queue_db.get_next_active_task()
_print_json("next_task", next_task)
queue_db.close()

task_key = next_task["task_key"]
queue_db.open()
queue_db.start_task(task_key)
queue_db.finish_task(task_key)

finish_code = random.randint(0, 100)
finish_log = {
	"access_token": "{:06d}".format(random.randint(0, 999999))
}
queue_db.amend_task(task_key, finish_code, finish_log)
queue_db.close()
