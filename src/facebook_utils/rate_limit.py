#!/usr/bin/env python3

from facebook_utils import RateLimitDB

from datetime import datetime, timedelta
import math
import time

# Facebook GraphAPI rate limit constants
DURATION = timedelta(minutes = 15, seconds = 0)
REQUESTS_PER_DURATION = 50
SECONDS_PER_DURATION = DURATION.total_seconds()

# Throttling constants
REQUESTS_THRESHOLD_75 = 0.75 * REQUESTS_PER_DURATION
REQUESTS_THRESHOLD_50 = 0.50 * REQUESTS_PER_DURATION
REQUESTS_THRESHOLD_25 = 0.25 * REQUESTS_PER_DURATION
REQUESTS_THRESHOLD_10 = 0.10 * REQUESTS_PER_DURATION
DELAY_MIN = 5.0
DELAY_SECONDS_75  =  5.0 # 12 *  5 =  60 seconds or  1/15 for  25% bandwidth
DELAY_SECONDS_50  = 10.0 # 12 * 10 = 120 seconds or  3/15 for  50% bandwidth
DELAY_SECONDS_25  = 15.0 # 12 * 15 = 180 seconds or  6/15 for  75% bandwidth
DELAY_SECONDS_10  = 20.0 #  9 * 20 = 180 seconds or  9/15 for  90% bandwidth
                         #                          15/15 for 100% bandwidth

class RateLimitManager:
	def __init__(self, db_folder = None, verbose = True):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self._db = RateLimitDB(db_folder = db_folder, verbose = False)

	def before_search(self):
		delay = self._calculate_delay()
		self._sleep(delay)

	def after_search(self):
		self._update_rate_limit()

	def _calculate_delay(self):
		self._db.open()
		usage_data = self._db.check_usage(duration = DURATION)
		self._db.close()

		if self.verbose:
			print()
			print("[RateLimitManager] Checking available bandwidth...")
			if usage_data.count == 0:
				prior_requests = "no prior requests"
			elif usage_data.count == 1:
				prior_requests = "1 prior request"
			else:
				prior_requests = "{:d} prior request".format(usage_data.count)
			print("    Found {:s} within the past {:0.1f} seconds".format(prior_requests, usage_data.duration))

		remaining_request_count = REQUESTS_PER_DURATION - usage_data.count
		remaining_request_duration = SECONDS_PER_DURATION - usage_data.duration

		if self.verbose:
			print("    Remaining time = {:0.1f} seconds".format(remaining_request_duration))
			print("    Remaining requests = {:d}".format(remaining_request_count))
			print("    Remaining bandwidth = {:0.1f}%".format(100.0 * remaining_request_count / REQUESTS_PER_DURATION))

		delay = remaining_request_duration / remaining_request_count if remaining_request_count >= 1 else remaining_request_duration
		if remaining_request_count >= REQUESTS_THRESHOLD_75:
			delay = min(delay, DELAY_SECONDS_75)
		if remaining_request_count >= REQUESTS_THRESHOLD_50:
			delay = min(delay, DELAY_SECONDS_50)
		if remaining_request_count >= REQUESTS_THRESHOLD_25:
			delay = min(delay, DELAY_SECONDS_25)
		if remaining_request_count >= REQUESTS_THRESHOLD_10:
			delay = min(delay, DELAY_SECONDS_10)
		delay = max(DELAY_MIN, delay)

		if self.verbose:
			print("    Delay = {:0.1f} second{:s}".format(delay, "" if delay == 1 else "s"))

		return delay

	def _sleep(self, delay):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[RateLimitManager] Sleeping for {:0.1f} second{:s} ({:s})...".format(delay, "" if delay == 1 else "s", timestamp))

		remaining_seconds = delay
		while remaining_seconds >= 60:
			remaining_minutes = math.floor(remaining_seconds / 60)
			if self.verbose:
				timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
				print("    Sleep for {:d} more minute{:s} ({:s})...".format(remaining_minutes, "" if remaining_minutes == 1 else "s", timestamp))

			time.sleep(60)
			remaining_seconds -= 60
		time.sleep(remaining_seconds)

		if self.verbose:
			print()

	def _update_rate_limit(self):
		if self.verbose:
			print()
			print("[RateLimitManager] Updating usage log...")
			print()
		self._db.open()
		self._db.add_timestamp()
		self._db.close()
