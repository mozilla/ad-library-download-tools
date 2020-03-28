#!/usr/bin/env python3

from common import Constants

import csv
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import math
import os
import sqlite3

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
	def __init__(self, export_to_db = True, save_json = True, save_csv = True, verbose = True):
		self.export_to_db = export_to_db
		self.save_json = save_json
		self.save_csv = save_csv
		self.verbose = verbose
		self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
		os.makedirs(self._get_download_folder(), exist_ok = True)
		os.makedirs(self._get_export_folder(), exist_ok = True)

	def _get_service_account_key_path(self):
		return os.path.join(Constants.PREF_PATH, SERVICE_ACCOUNT_KEY_JSON)

	def _get_download_folder(self):
		return os.path.join(Constants.DOWNLOADS_PATH, "google", self.timestamp)

	def _get_export_folder(self):
		return os.path.join(Constants.EXPORTS_PATH, "google", self.timestamp)

	def _get_client(self):
		service_account_key = self._get_service_account_key_path()
		credentials = service_account.Credentials.from_service_account_file(service_account_key, scopes = SCOPES)
		client = bigquery.Client(credentials = credentials, project = credentials.project_id)
		return client

	def _get_create_table_sql(self, table, columns):
		table_name = table.split(".")[-1]
		sql_prefix = """CREATE TABLE IF NOT EXISTS "{}" (""".format(table_name)
		sql_fields = [""""key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE"""]
		for column in columns:
			field_name = column["name"]
			field_type = column["field_type"]
			field_is_nullable = "" if column["is_nullable"] else "NOT NULL"
			sql_fields.append(""""{}" {} {}""".format(field_name, field_type, field_is_nullable))
		sql_suffix = """);"""
		sql = "\n".join([sql_prefix, ",\n".join(sql_fields), sql_suffix])
		return sql

	def _get_insert_row_sql(self, table, columns):
		table_name = table.split(".")[-1]
		sql_prefix = """INSERT INTO "{}" (""".format(table_name)
		sql_fields = []
		sql_bridge = """) VALUES ("""
		sql_values = []
		sql_suffix = """);"""
		for column in columns:
			sql_fields.append(column["name"])
			sql_values.append("?")
		sql = "\n".join([sql_prefix, ", ".join(sql_fields), sql_bridge, ", ".join(sql_values), sql_suffix])
		return sql

	def download_all_tables(self):
		for table in TABLES:
			self.download_table(table)

	def download_table(self, table):
		start_timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		
		if self.verbose:
			print("[GoogleCloudHelper] Download task started at {:s}".format(start_timestamp))
			print("[GoogleCloudHelper] Table = {:s}".format(table))

		if self.export_to_db:
			db_filename = os.path.join(self._get_export_folder(), "google_exports.sqlite")
			connection = sqlite3.connect(db_filename, detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
			connection.row_factory = sqlite3.Row
			cursor = connection.cursor()
			if self.verbose:
				print("[GoogleCloudHelper] SQLite DB = {:s}".format(db_filename))

		if self.save_json:
			json_filename = os.path.join(self._get_download_folder(), "{}.txt".format(table))
			json_fp = open(json_filename, "w")
			if self.verbose:
				print("[GoogleCloudHelper] JSON file = {:s}".format(json_filename))

		if self.save_csv:
			csv_filename = os.path.join(self._get_download_folder(), "{}.csv".format(table))
			csv_fp = open(csv_filename, "w")
			csv_writer = csv.writer(csv_fp, delimiter = "\t", quotechar = '\"', quoting=csv.QUOTE_MINIMAL)
			if self.verbose:
				print("[GoogleCloudHelper] CSV file = {:s}".format(csv_filename))

		client = self._get_client()
		rows = client.list_rows(table, page_size = 10000)

		# Save table column header
		columns = [{
			"name": column.name,
			"description": column.description,
			"field_type": column.field_type,
			"is_nullable": column.is_nullable,
		} for column in rows.schema]
		if self.export_to_db:
			create_table_sql = self._get_create_table_sql(table, columns)
			insert_row_sql = self._get_insert_row_sql(table, columns)
			cursor.execute(create_table_sql)
		if self.save_json:
			json.dump(columns, json_fp, separators = (",", ":"), default = str)
			json_fp.write("\n")
		if self.save_csv:
			csv_writer.writerow([column["name"] for column in columns])

		# Save table rows
		for page in rows.pages:
			if self.verbose:
				print("[GoogleCloudHelper] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				if self.export_to_db:
					cursor.execute(insert_row_sql, tuple(list(row)))
				if self.save_json:
					json.dump(list(row), json_fp, separators = (",", ":"), default = str)
					json_fp.write("\n")
				if self.save_csv:
					csv_writer.writerow(list(row))

		if self.export_to_db:
			connection.commit()
			connection.close()
		if self.save_json:
			json_fp.close()
		if self.save_csv:
			csv_fp.close()

		end_timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		if self.verbose:
			print("[GoogleCloudHelper] Download task completed at {:s}".format(end_timestamp))
			print()
