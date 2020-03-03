#!/usr/bin/env python

from datetime import datetime
import json
import requests
import urllib.parse

DASH = "--------------------------------------------------------------------------------"
URL_BASE = "https://graph.facebook.com/v4.0/ads_archive"

class FacebookAPI:
	def __init__(self, verbose = False):
		self.verbose = verbose
	
	def get_url(self, this_task, access_token):
		experiment_spec = this_task["experiment_spec"]
		split_spec = this_task["split_spec"]
		page_spec = this_task["page_spec"]
		attempt_spec = this_task["attempt_spec"]
		continuation = this_task["continuation"]
		
		all_specs = {**experiment_spec, **split_spec, **page_spec, **attempt_spec, **continuation}
		ad_type = all_specs["ad_type"]
		ad_active_status = all_specs["ad_active_status"]
		ad_fields = all_specs["ad_fields"]
		ads_per_page = all_specs["ads_per_page"]
		countries = all_specs["countries"]
		search_terms = all_specs["search_terms"]
		advertisers = all_specs["advertisers"]
		search_by_advertisers = all_specs["search_by_advertisers"]
		platforms = all_specs["platforms"]
		last_n_days = all_specs["last_n_days"]

		after_token = continuation["after_token"] if "after_token" in continuation else None
		
		data = {
			"access_token": access_token,
			"ad_type": ad_type,
			"ad_active_status": ad_active_status,
			"fields": ",".join(ad_fields),
			"platforms": ",".join(platforms),
			"ad_reached_countries": ",".join(countries),
			"limit": ads_per_page,
		}
		if search_by_advertisers:
			data["search_page_ids"] = ",".join(advertisers)
		else:
			data["search_terms"] = " ".join(search_terms)
		if last_n_days > 0:
			if last_n_days <= 1:
				data["impression_condition"] = "HAS_IMPRESSIONS_YESTERDAY"
			elif last_n_days <= 7:
				data["impression_condition"] = "HAS_IMPRESSIONS_LAST_7_DAYS"
			elif last_n_days <= 30:
				data["impression_condition"] = "HAS_IMPRESSIONS_LAST_30_DAYS"
			elif last_n_days <= 90:
				data["impression_condition"] = "HAS_IMPRESSIONS_LAST_90_DAYS"
			else:
				data["impression_condition"] = "HAS_IMPRESSIONS_LIFETIME"
		else:
			data["impression_condition"] = "HAS_IMPRESSIONS_LIFETIME"
		if after_token is not None:
			data["after"] = after_token
		
		url = "{}?{}".format(URL_BASE, urllib.parse.urlencode(data))
		return url

	def search(self, url):
		request_timestamp = datetime.now()
		try:
			r = requests.get(url)
		except requests.exceptions.ConnectionError as e:
			return {
				"request_timestamp": request_timestamp,
				"response_timestamp": None,
				"duration": None,
				"response_header": None,
				"response_body": None,
				"response_html": None,
				"response_error": str(e),
			}
			
		response_timestamp = datetime.now()
		duration = (response_timestamp - request_timestamp).total_seconds()
		response_header = dict(r.headers)
		try:
			response_body = r.json()
			return {
				"request_timestamp": request_timestamp,
				"response_timestamp": response_timestamp,
				"duration": duration,
				"response_header": response_header,
				"response_body": response_body,
				"response_html": None,
				"response_error": None,
			}
		except json.decoder.JSONDecodeError:
			response_html = r.text
			return {
				"request_timestamp": request_timestamp,
				"response_timestamp": response_timestamp,
				"duration": duration,
				"response_header": response_header,
				"response_body": None,
				"response_html": response_html,
				"response_error": None,
			}

	def parse_results(self, this_task, access_token, results):
		response_error = results["response_error"]
		
		# Connection error
		if response_error is not None:
			ad_count = 0
			finish_code = -10001
			finish_log = {
				"note": "Failed on: requests.get(url)",
				"response": response_error,
			}
			finish_log["access_token"] = access_token
			finish_log["continuation"] = this_task["continuation"].copy()
			print()
			print(DASH)
			print("[facebook-api] EXCEPTION: Requests connection error")
			print(DASH)
			print()
			return (finish_code, finish_log)
		
		# Received an HTML document instead of a JSON object
		response_html = results["response_html"]
		if response_html is not None:
			ad_count = 0
			finish_code = -10002
			finish_log = {
				"note": "Failed on: response.json()",
				"response": response_html,
			}
			finish_log["access_token"] = access_token
			finish_log["continuation"] = this_task["continuation"].copy()
			print()
			print(DASH)
			print("[facebook-api] EXCEPTION: Received an HTML document instead of a JSON object")
			print(DASH)
			print()
			return (finish_code, finish_log)

		response_body = results["response_body"]
		finish_log = self._parse_response_body(response_body)
		finish_log["access_token"] = access_token

		# Response JSON object contains data
		if finish_log["has_data"]:
			
			# Continue to next page
			if finish_log["has_paging_next_cursor"]:
				finish_code = 0
				finish_log["continuation"] = this_task["continuation"].copy()
				finish_log["continuation"]["after_token"] = finish_log["paging_cursor"]
				finish_log["continuation"]["total_ad_count"] = finish_log["ad_count"] + (finish_log["continuation"]["total_ad_count"] if "total_ad_count" in finish_log["continuation"] else 0)

				print()
				print(DASH)
				print("[facebook-api] DATA: Received a page of {:,} ads ({:,} total ads)".format(finish_log["ad_count"], finish_log["continuation"]["total_ad_count"]))
				print(DASH)
				print()
			
			# Terminal page
			else:
				finish_code = -1
				finish_log["continuation"] = this_task["continuation"].copy()
				finish_log["continuation"]["total_ad_count"] = finish_log["ad_count"] + (finish_log["continuation"]["total_ad_count"] if "total_ad_count" in finish_log["continuation"] else 0)

				print()
				print(DASH)
				print("[facebook-api] DATA: Received final page of {:,} ads ({:,} total ads)".format(finish_log["ad_count"], finish_log["continuation"]["total_ad_count"]))
				print(DASH)
				print()
		
		# Response JSON object does not contain data
		else:
			
			# Response JSON object contains an error code
			if finish_log["has_error"]:
				finish_code = finish_log["error_code"]
				finish_log["continuation"] = this_task["continuation"].copy()
			
				print()
				print(DASH)
				print("[facebook-api] ERROR {}: {}".format(finish_log["error_code"], finish_log["error_message"]))
				print(DASH)
				print()
			
			# Response JSON object contains neither data nor erorr codes
			else:
				finish_code = -10003
				
				print()
				print(DASH)
				print("[facebook-api] UNKNOWN ERROR")
				print(DASH)
				print()

		return (finish_code, finish_log)
	
	def _parse_response_body(self, response_body):
		has_data = False
		has_paging = False
		has_paging_cursors = False
		has_paging_next_cursor = False
		has_error = False
		ad_count = 0
		paging_cursor = None
		error_code = None
		error_message = None
		if "data" in response_body:
			has_data = True
			ad_count = len(response_body["data"])
		if "paging" in response_body:
			has_paging = True
			if "cursors" in response_body["paging"]:
				has_paging_cursors = True
				if "after" in response_body["paging"]["cursors"]:
					has_paging_next_cursor = True
					paging_cursor = response_body["paging"]["cursors"]["after"]
		if "error" in response_body:
			has_error = True
			if "code" in response_body["error"]:
				error_code = response_body["error"]["code"]
				error_message = response_body["error"]["message"]
		return {
			"has_data": has_data,
			"has_paging": has_paging,
			"has_paging_cursors": has_paging_cursors,
			"has_paging_next_cursor": has_paging_next_cursor,
			"has_error": has_error,
			"ad_count": ad_count,
			"paging_cursor": paging_cursor,
			"error_code": error_code,
			"error_message": error_message,
		}
