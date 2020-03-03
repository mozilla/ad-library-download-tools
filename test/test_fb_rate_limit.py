#!/usr/bin/env python3

import facebook_utils

import time
import random

rate_limit_manager = facebook_utils.RateLimitManager(db_folder = "../db/test", verbose = True)

for i in range(0, 1000):
	rate_limit_manager.before_search()
	print("...")
	time.sleep(random.randint(1, 20) * 0.1)
	print("...")
	rate_limit_manager.after_search()
