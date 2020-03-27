#!/usr/bin/env python3

from common import Constants

import csv
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import math
import os

SERVICE_ACCOUNT_KEY_JSON = "google_service_account_key.json"

SCOPES = [
	"https://www.googleapis.com/auth/bigquery.readonly"
]

TABLES = [
	"bigquery-public-data.google_political_ads.advertiser_stats",
	"bigquery-public-data.google_political_ads.advertiser_weekly_spend",
	"bigquery-public-data.google_political_ads.campaign_targeting",
	"bigquery-public-data.google_political_ads.creative_stats",
	"bigquery-public-data.google_political_ads.geo_spend",
	"bigquery-public-data.google_political_ads.top_keywords_history",
]

class GoogleCloudHelper:
	def __init__(self, export_to_db = True, save_json = True, save_csv = True, verbose = False):
		self.export_to_db = export_to_db
		self.save_json = save_json
		self.save_csv = save_csv
		self.verbose = verbose
		self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
		os.makedirs(self.get_download_folder(), exist_ok = True)
		os.makedirs(self.get_export_folder(), exist_ok = True)
	
	def get_service_account_key_path(self):
		return os.path.join(Constants.PREF_PATH, SERVICE_ACCOUNT_KEY_JSON)
	
	def get_download_folder(self):
		return os.path.join(Constants.DOWNLOADS_PATH, "google", self.timestamp)
	
	def get_export_folder(self):
		return os.path.join(Constants.EXPORTS_PATH)
	
	def get_client(self):
		service_account_key = self.get_service_account_key_path()
		credentials = service_account.Credentials.from_service_account_file(service_account_key, scopes = SCOPES)
		client = bigquery.Client(credentials = credentials, project = credentials.project_id)
		return client
	
	def download_all_tables(self):
		for table in TABLES:
			self.download_table(table)

	def download_table(self, table):
		start_timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		print("[GoogleCloudHelper] Download task started at {:s}".format(start_timestamp))
		print("[GoogleCloudHelper] Table = {}".format(table))

		if self.export_to_db:
			pass
		if self.save_json:
			json_filename = os.path.join(self.get_download_folder(), "{}.txt".format(table))
			json_fp = open(json_filename, "w")
			print("[GoogleCloudHelper] JSON file = {}".format(json_filename))

		if self.save_csv:
			csv_filename = os.path.join(self.get_download_folder(), "{}.csv".format(table))
			csv_fp = open(csv_filename, "w")
			csv_writer = csv.writer(csv_fp, delimiter = "\t", quotechar = '\"', quoting=csv.QUOTE_MINIMAL)
			print("[GoogleCloudHelper] CSV file = {}".format(csv_filename))

		client = self.get_client()
		rows = client.list_rows(table, page_size = 10000)

		# Save table column header
		columns = [{
			"name": column.name,
			"description": column.description,
			"field_type": column.field_type,
			"is_nullable": column.is_nullable,
		} for column in rows.schema]
		if self.save_json:
			json.dump(columns, json_fp, separators = (",", ":"), default = str)
			json_fp.write("\n")
		if self.save_csv:
			csv_writer.writerow([column["name"] for column in columns])

		# Save table rows
		for page in rows.pages:
			print("[GoogleCloudHelper] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				if self.save_json:
					json.dump(list(row), json_fp, separators = (",", ":"), default = str)
					json_fp.write("\n")
				if self.save_csv:
					csv_writer.writerow(list(row))

		if self.export_to_db:
			pass
		if self.save_json:
			json_fp.close()
		if self.save_csv:
			csv_fp.close()

		end_timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		print("[GoogleCloudHelper] Download task completed at {:s}".format(end_timestamp))
		print()
