#!/usr/bin/env python3

from common import Constants

import configparser
import csv
from datetime import datetime
import math
import os
import random

# Configuration files
TASK_CONFIG_FILENAME = os.path.join(Constants.PREF_PATH, "facebook_tasks.ini")
DEFAULT_TASK_CONFIG_FILENAME = os.path.join("facebook_utils", "defaults", "tasks.ini")

# Constants for dynamically checking the current rate limit
MAX_ADS_PER_PAGE = 5000
MIN_ADS_PER_PAGE = 25
ADS_PER_PAGE_INCREASE_FACTOR = 1.189207115   # 2.0 ^ (1/4)
ADS_PER_PAGE_DECREASE_FACTOR = 0.70710678118 # 0.5 ^ (1/2)
RAND_ADD_ADS = 25
RAND_SUBTRACT_ADS = 25
RAND_MULTIPLY_ADS = 1.025
RAND_DIVIDE_ADS = 1.025

# Fail condition
TASK_FAILS_AFTER_N_ATTEMPTS = 10

class TaskManager:
	def __init__(self, verbose = False):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self._init_config()

	def _init_config(self):
		filename = TASK_CONFIG_FILENAME
		if not os.path.exists(filename):
			config = configparser.ConfigParser()
			config.read(DEFAULT_TASK_CONFIG_FILENAME)
			with open(filename, "w") as f:
				config.write(f)
			if self.verbose:
				print("[TaskManager] Created file: {}".format(filename))

	def _read_advertisers_from_report_csv(self, filename):
		header = None
		advertisers = []
		with open(filename) as f:
			reader = csv.reader(f, delimiter=",", quotechar="\"", quoting=csv.QUOTE_MINIMAL)
			for row in reader:
				if header is None:
					header = row[0]
					# Facebook inserts a utf-16 character at the start of the CSV file
					header = header.lstrip("\uFEFF")
					assert (header == "Page ID")
				else:
					advertisers.append(row[0])
		return advertisers

	def create_experiment(self, experiment_type, experiment_priority = None):
		assert isinstance(experiment_type, str)
		config = configparser.ConfigParser()
		config.read(TASK_CONFIG_FILENAME)

		experiment_section = experiment_type.upper()
		task_priority = config.getint(experiment_section, "task_priority")
		ad_type = config.get(experiment_section, "ad_type")
		ad_active_status = config.get(experiment_section, "ad_active_status")
		ad_fields = config.get(experiment_section, "ad_fields")
		countries = config.get(experiment_section, "countries")
		search_terms = config.get(experiment_section, "search_terms")
		advertisers = config.get(experiment_section, "advertisers")
		advertisers_from_report = config.get(experiment_section, "advertisers_from_report")
		platforms = config.get(experiment_section, "platforms")
		last_n_days = config.getint(experiment_section, "last_n_days")
		ads_per_page = config.getint(experiment_section, "ads_per_page")
		countries_per_split = config.getint(experiment_section, "countries_per_split")
		advertisers_per_split = config.getint(experiment_section, "advertisers_per_split")
		search_by_advertisers = config.getboolean(experiment_section, "search_by_advertisers")

		root_folder = config.get(experiment_section, "root_folder") if config.has_option(experiment_section, "root_folder") else Constants.DATA_PATH
		now = datetime.now()
		timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
		experiment_key = experiment_type.lower()
		experiment_folder = "{}/facebook--{}--{}".format(root_folder, experiment_type.lower(), timestamp)

		ad_fields = ad_fields.split(",")
		countries = countries.split(",")
		search_terms = [] if len(search_terms) == 0 else search_terms.split(",")
		advertisers = [] if len(advertisers) == 0 else advertisers.split(",")
		platforms = [] if len(platforms) == 0 else platforms.split(",")

		if experiment_priority is not None:
			task_priority = experiment_priority

		experiment_spec = {
			"experiment_key": experiment_key,
			"experiment_folder": experiment_folder,
			"task_priority": task_priority,
			"ad_type": ad_type,
			"ad_active_status": ad_active_status,
			"ad_fields": ad_fields,
			"countries": countries,
			"search_terms": search_terms,
			"advertisers": advertisers,
			"advertisers_from_report": advertisers_from_report,
			"platforms": platforms,
			"last_n_days": last_n_days,
			"ads_per_page": ads_per_page,
			"countries_per_split": countries_per_split,
			"advertisers_per_split": advertisers_per_split,
			"search_by_advertisers": search_by_advertisers,
		}
		return experiment_spec

	def create_splits(self, experiment_spec):
		split_specs = []
		search_by_advertisers = experiment_spec["search_by_advertisers"]
		if search_by_advertisers:
			advertisers = experiment_spec["advertisers"].copy()
			advertisers_from_report = experiment_spec["advertisers_from_report"]
			if len(advertisers_from_report) > 0:
				advertisers += self._read_advertisers_from_report_csv(advertisers_from_report)

			advertisers_per_split = experiment_spec["advertisers_per_split"]
			advertiser_count = len(advertisers)
			split_count = math.ceil(1.0 * advertiser_count / advertisers_per_split)
			for split_index in range(0, split_count):
				start_index = split_index * advertisers_per_split
				end_index = (split_index+1) * advertisers_per_split
				split_advertisers = advertisers[start_index:end_index]
				split_spec = {
					"split_index": split_index,
					"split_count": split_count,
					"advertisers": split_advertisers,
				}
				split_specs.append(split_spec)
		else:
			countries = experiment_spec["countries"]
			countries_per_split = experiment_spec["countries_per_split"]
			country_count = len(countries)
			split_count = math.ceil(1.0 * country_count / countries_per_split)
			for split_index in range(0, split_count):
				start_index = split_index * countries_per_split
				end_index = (split_index+1) * countries_per_split
				split_countries = countries[start_index:end_index]
				split_spec = {
					"split_index": split_index,
					"split_count": split_count,
					"countries": split_countries,
				}
				split_specs.append(split_spec)
		return split_specs

	def init_page(self):
		page_spec = {
			"page_index": 0,
		}
		return page_spec

	def init_attempt(self):
		attempt_spec = {
			"page_attempt": 0,
			"attempt_index": 0,
		}
		return attempt_spec

	def init_continuation(self):
		continuation = {}
		return continuation

	def continue_task(self, this_task, finish_code, finish_log):
		experiment_spec = this_task["experiment_spec"].copy()
		split_spec = this_task["split_spec"].copy()
		page_spec = this_task["page_spec"].copy()
		attempt_spec = this_task["attempt_spec"].copy()
		continuation = finish_log["continuation"].copy() if "continuation" in finish_log else {}

		all_specs = {**experiment_spec, **split_spec, **page_spec, **attempt_spec, **continuation}
		ads_per_page = all_specs["ads_per_page"]

		# Success
		if finish_code == 0:
			page_spec["page_index"] += 1
			attempt_spec["attempt_index"] += 1
			attempt_spec["page_attempt"] = 0
			if "error_codes" in continuation:
				del continuation["error_codes"]
			if "is_task_failed" in continuation:
				del continuation["is_task_failed"]

			ads_per_page = ads_per_page * ADS_PER_PAGE_INCREASE_FACTOR
			x1 = ads_per_page * random.uniform(1.0, RAND_MULTIPLY_ADS) - ads_per_page
			x2 = ads_per_page / random.uniform(1.0, RAND_DIVIDE_ADS) - ads_per_page
			x3 = random.randint(0, RAND_ADD_ADS)
			x4 = -random.randint(0, RAND_SUBTRACT_ADS)
			ads_per_page = math.ceil(ads_per_page + x1 + x2 + x3 + x4)
			ads_per_page = min(MAX_ADS_PER_PAGE, max(MIN_ADS_PER_PAGE, ads_per_page))
			attempt_spec["ads_per_page"] = ads_per_page

			this_task = {
				"experiment_spec": experiment_spec,
				"split_spec": split_spec,
				"page_spec": page_spec,
				"attempt_spec": attempt_spec,
				"continuation": continuation,
			}
			return this_task

		# Retry using the same set of parameters

		# 190 = expired access token
		elif finish_code == 190:
			attempt_spec["attempt_index"] += 1
			attempt_spec["page_attempt"] += 1
			if "error_codes" not in continuation:
				continuation["error_codes"] = []
			continuation["error_codes"].append(finish_code)
			continuation["is_task_failed"] = True

			this_task = {
				"experiment_spec": experiment_spec,
				"split_spec": split_spec,
				"page_spec": page_spec,
				"attempt_spec": attempt_spec,
				"continuation": continuation,
			}
			return this_task

		# Failure and retry with fewer ads
		elif finish_code > 0:
			attempt_spec["attempt_index"] += 1
			attempt_spec["page_attempt"] += 1
			if "error_codes" not in continuation:
				continuation["error_codes"] = []
			continuation["error_codes"].append(finish_code)

			# Failed more than N times
			if attempt_spec["page_attempt"] >= TASK_FAILS_AFTER_N_ATTEMPTS:
				continuation["is_task_failed"] = True
				this_task = {
					"experiment_spec": experiment_spec,
					"split_spec": split_spec,
					"page_spec": page_spec,
					"attempt_spec": attempt_spec,
					"continuation": continuation,
				}
				return this_task
			else:
				ads_per_page = ads_per_page * ADS_PER_PAGE_DECREASE_FACTOR
				x1 = ads_per_page * random.uniform(1.0, RAND_MULTIPLY_ADS) - ads_per_page
				x2 = ads_per_page / random.uniform(1.0, RAND_DIVIDE_ADS) - ads_per_page
				x3 = random.randint(0, RAND_ADD_ADS)
				x4 = -random.randint(0, RAND_SUBTRACT_ADS)
				ads_per_page = math.floor(ads_per_page + x1 + x2 + x3 + x4)
				ads_per_page = min(MAX_ADS_PER_PAGE, max(MIN_ADS_PER_PAGE, ads_per_page))
				attempt_spec["ads_per_page"] = ads_per_page
				this_task = {
					"experiment_spec": experiment_spec,
					"split_spec": split_spec,
					"page_spec": page_spec,
					"attempt_spec": attempt_spec,
					"continuation": continuation,
				}
				return this_task

		# Terminal page
		elif finish_code == -1:
			return None

		# Other network errors
		else:
			attempt_spec["attempt_index"] += 1
			attempt_spec["page_attempt"] += 1
			if "error_codes" not in continuation:
				continuation["error_codes"] = []
			continuation["error_codes"].append(finish_code)

			# Failed more than N times
			if attempt_spec["page_attempt"] >= TASK_FAILS_AFTER_N_ATTEMPTS:
				continuation["is_task_failed"] = True
				this_task = {
					"experiment_spec": experiment_spec,
					"split_spec": split_spec,
					"page_spec": page_spec,
					"attempt_spec": attempt_spec,
					"continuation": continuation,
				}
				return this_task
			else:
				this_task = {
					"experiment_spec": experiment_spec,
					"split_spec": split_spec,
					"page_spec": page_spec,
					"attempt_spec": attempt_spec,
					"continuation": continuation,
				}
				return this_task
