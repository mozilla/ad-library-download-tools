#!/usr/bin/env python3

import facebook_utils

rate_limit_manager = facebook_utils.RateLimitManager(db_folder = "../db/test", verbose = True)

for i in range(0, 1000):
	rate_limit_manager.before_search()
	print("...")
	rate_limit_manager.after_search()