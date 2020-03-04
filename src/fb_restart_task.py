#!/usr/bin/env python3

import facebook_utils
import argparse

parser = argparse.ArgumentParser(usage = "Restart a task in the download queue.")
parser.add_argument("task_key", type = int)

# Parse command line arguments.
args = parser.parse_args()

# Restart a task.
queue_manager = facebook_utils.QueueManager(verbose = True)
queue_manager.restart_task(args.task_key)
