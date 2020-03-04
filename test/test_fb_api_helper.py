#!/usr/bin/env python3

import facebook_utils

import argparse
import json

def print_json(label, data):
	print("[{}]".format(label))
	print(json.dumps(data, indent = 2))
	print()

parser = argparse.ArgumentParser()
parser.add_argument("task_key", type = int, nargs = "?")
parser.add_argument("--execute", action = "store_const", const = True, default = False)
args = parser.parse_args()

token_manager = facebook_utils.TokenManager()
queue_manager = facebook_utils.QueueManager(db_folder = "../db/test")
task_manager = facebook_utils.TaskManager()

access_token = token_manager.get_user_access_token()

print("[access_token]")
print(access_token)
print()

task_key = args.task_key
if task_key is None:
	task = queue_manager.get_next_active_task()
	task_key = task["task_key"]
else:
	task = queue_manager.get_task(task_key)

print_json("task", task)

api_helper = facebook_utils.APIHelper()
url = api_helper.get_url(task, access_token)

print("[url]")
print(url)
print()

if args.execute:
	queue_manager.start_task(task_key)

	response = api_helper.search(url)
	print("[response]")
	print(response)
	print()
	
	queue_manager.finish_task(task_key)

	(finish_code, finish_log) = api_helper.parse_response(task, access_token, response)
	print("[finish_code]")
	print(finish_code)
	print()
	print_json("finish_log", finish_log)
	
	queue_manager.amend_task(task_key, finish_code, finish_log)
	
	next_task = task_manager.continue_task(task, finish_code, finish_log)
	print_json("next_task", next_task)

	if next_task is not None:
		experiment_spec = next_task["experiment_spec"]
		split_spec = next_task["split_spec"]
		page_spec = next_task["page_spec"]
		attempt_spec = next_task["attempt_spec"]
		continuation = next_task["continuation"]
		queue_manager.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
