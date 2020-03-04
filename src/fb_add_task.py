#!/usr/bin/env python3

import facebook_utils
import argparse

DEFAULT_COUNTRY = "us"
COUNTRIES = [
	"us", "ca",
	"eu", "at", "be", "bg", "hr", "cy", "cz", "dk", "ee", "fi", "fr", "de", "gr", "hu", "ie", "it", "lv", "lt", "lu", "mt", "nl", "pl", "pt", "ro", "sk", "si", "es", "se", "uk",
	"latam", "ar", "bo", "br", "cl", "co", "ec", "fk", "gf", "gy", "py", "pe", "sr", "uy", "ve",
	"il", "in", "ua"
]

DEFAULT_DURATION = "1"
DURATIONS = [
	"1", "7", "30", "90", "all"
]

parser = argparse.ArgumentParser(
	usage = "Add a task to the download queue.",
	description = "This script adds a task to download ad data from the Facebook Ad Library. Call 'fb_run_tasks.py' to start the task once it's been added to the queue."
)
parser.add_argument("country", choices = COUNTRIES, type = str, default = DEFAULT_COUNTRY)
parser.add_argument("last_n_days", choices = DURATIONS, type = str, default = DEFAULT_DURATION)

# Parse command line arguments.
args = parser.parse_args()
experiment_type = args.country
last_n_days = -1 if args.last_n_days == "all" else int(args.last_n_days)

task_manager = facebook_utils.TaskManager(verbose = True)
queue_manager = facebook_utils.QueueManager(verbose = True)

# Create task(s).
experiment_spec = task_manager.create_experiment(experiment_type, last_n_days)
split_specs = task_manager.create_splits(experiment_spec)
page_spec = task_manager.init_page()
attempt_spec = task_manager.init_attempt()
continuation = task_manager.init_continuation()

# Add task(s) to queue.
queue_manager.create_tasks(experiment_spec, split_specs, page_spec, attempt_spec, continuation)
