#!/usr/bin/env python3

import facebook_utils

import json

def _print_json(label, data):
	print("[{}]".format(label))
	print(json.dumps(data, indent = 2))
	print()

for experiment_type in ["us", "uk", "eu", "eu28", "us-advertisers"]:
	task_manager = facebook_utils.TaskManager(verbose = True)
	experiment_spec = task_manager.create_experiment(experiment_type)
	_print_json("experiment_spec", experiment_spec)

	split_specs = task_manager.create_splits(experiment_spec)
	for split_spec in split_specs:
		_print_json("split_spec", split_spec)

		page_spec = task_manager.init_page()
		_print_json("page_spec", page_spec)

		attempt_spec = task_manager.init_attempt()
		_print_json("attempt_spec", attempt_spec)

		continuation = task_manager.init_continuation()
		_print_json("continuation", continuation)
