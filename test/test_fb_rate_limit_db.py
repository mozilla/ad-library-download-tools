#!/usr/bin/env python3

import facebook_utils

from datetime import timedelta
import time

rate_limit_db = facebook_utils.RateLimitDB(db_folder = "../db/test", verbose = True)
rate_limit_db.open()
rate_limit_db.check_usage()
rate_limit_db.add_timestamp()
rate_limit_db.check_usage()
time.sleep(1)
rate_limit_db.add_timestamp()
rate_limit_db.check_usage()
time.sleep(2)
rate_limit_db.add_timestamp()
rate_limit_db.check_usage()
time.sleep(5)
rate_limit_db.add_timestamp()
rate_limit_db.check_usage()
time.sleep(5)
rate_limit_db.add_timestamp()
rate_limit_db.check_usage()
rate_limit_db.check_usage(timedelta(minutes = 5))
rate_limit_db.check_usage(timedelta(minutes = 2))
rate_limit_db.check_usage(timedelta(minutes = 1))
rate_limit_db.check_usage(timedelta(seconds = 15))
rate_limit_db.check_usage(timedelta(seconds = 5))
rate_limit_db.close()
