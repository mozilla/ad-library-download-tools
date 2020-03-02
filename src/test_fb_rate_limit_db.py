#!/usr/bin/env python3

from datetime import timedelta
import facebook_utils
import time

rateLimitDB = facebook_utils.RateLimitDB(db_folder = "../db/test", verbose = True)
rateLimitDB.open()
rateLimitDB.check_usage()
rateLimitDB.add_timestamp()
rateLimitDB.check_usage()
time.sleep(1)
rateLimitDB.add_timestamp()
rateLimitDB.check_usage()
time.sleep(2)
rateLimitDB.add_timestamp()
rateLimitDB.check_usage()
time.sleep(5)
rateLimitDB.add_timestamp()
rateLimitDB.check_usage()
time.sleep(5)
rateLimitDB.add_timestamp()
rateLimitDB.check_usage()
rateLimitDB.check_usage(timedelta(minutes = 5))
rateLimitDB.check_usage(timedelta(minutes = 2))
rateLimitDB.check_usage(timedelta(minutes = 1))
rateLimitDB.check_usage(timedelta(seconds = 15))
rateLimitDB.check_usage(timedelta(seconds = 5))
rateLimitDB.close()
