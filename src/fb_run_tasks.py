#!/usr/bin/env python3

import facebook_utils
import argparse

MAX_ITERS = 99999

parser = argparse.ArgumentParser(
	usage = "Execute all tasks in the download queue.",
	description = "This script executes all tasks in the download queue. Call 'fb_add_task.py' to add download tasks."
)

api_helper = facebook_utils.APIHelper(verbose = True)
token_manager = facebook_utils.TokenManager(verbose = True)
task_manager = facebook_utils.TaskManager(verbose = True)
queue_manager = facebook_utils.QueueManager(verbose = True)
rate_limit_manager = facebook_utils.RateLimitManager(verbose = True)
downloads_db = facebook_utils.DownloadsDB(verbose = True)

for iter in range(0, MAX_ITERS):

	# Get the next active task
	task = queue_manager.get_next_active_task()
	
	if task is None:
		break
	else:
		task_key = task["task_key"]

		# Read the latest user access token.
		access_token = token_manager.get_user_access_token()

		# Construct the URL for the Graph API end point.
		url = api_helper.get_url(task, access_token)
	
		# Query the Graph API end point, obeying any rate limit.
		rate_limit_manager.before_search()
		queue_manager.start_task(task_key)
		response = api_helper.search(url)
		queue_manager.finish_task(task_key)
		(finish_code, finish_log) = api_helper.parse_response(task, access_token, response)
		queue_manager.amend_task(task_key, finish_code, finish_log)
		rate_limit_manager.after_search()
		
		# Save downloaded data.
		downloads_db.open()
		downloads_db.insert(queue_manager.get_task_as_dict(task_key), url, response)
		downloads_db.close()
		
		# Schedule a new task, if the download task is not completed.
		next_task = task_manager.continue_task(task, finish_code, finish_log)
		if next_task is not None:
			experiment_spec = next_task["experiment_spec"]
			split_spec = next_task["split_spec"]
			page_spec = next_task["page_spec"]
			attempt_spec = next_task["attempt_spec"]
			continuation = next_task["continuation"]
			queue_manager.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
