#!/usr/bin/env python3

from common import Constants

import csv
from datetime import datetime
import os
import sqlite3
import urllib.request
import zipfile

URL = "https://storage.googleapis.com/transparencyreport/google-political-ads-transparency-bundle.zip"
ZIP_FILENAME = "google-political-ads-transparency-bundle.zip"
CSV_FOLDER = "google-political-ads-transparency-bundle"
DB_FILENAME = "google_exports.sqlite"

DB_DEFS = [
	{
		"filename": "google-political-ads-advertiser-stats.csv",
		"table": "advertiser_stats",
		"sql": [
"""CREATE TABLE "advertiser_stats" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"advertiser_id" STRING,
	"advertiser_name" STRING,
	"public_ids_list" STRING,
	"regions" STRING,
	"elections" STRING,
	"total_creatives" INTEGER,
	"spend_usd" INTEGER,
	"spend_eur" INTEGER,
	"spend_inr" INTEGER,
	"spend_bgn" INTEGER,
	"spend_hrk" INTEGER,
	"spend_czk" INTEGER,
	"spend_dkk" INTEGER,
	"spend_huf" INTEGER,
	"spend_pln" INTEGER,
	"spend_ron" INTEGER,
	"spend_sek" INTEGER,
	"spend_gbp" INTEGER 
);""",
"""CREATE UNIQUE INDEX "advertiser_stats_index" ON "advertiser_stats" (
	"advertiser_id" ASC
);""",
		]
	}, {
		"filename": "google-political-ads-advertiser-weekly-spend.csv",
		"table": "advertiser_weekly_spend",
		"sql": [
"""CREATE TABLE "advertiser_weekly_spend" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"advertiser_id" STRING,
	"advertiser_name" STRING,
	"election_cycle" STRING,
	"week_start_date" DATE,
	"spend_usd" INTEGER,
	"spend_eur" INTEGER,
	"spend_inr" INTEGER,
	"spend_bgn" INTEGER,
	"spend_hrk" INTEGER,
	"spend_czk" INTEGER,
	"spend_dkk" INTEGER,
	"spend_huf" INTEGER,
	"spend_pln" INTEGER,
	"spend_ron" INTEGER,
	"spend_sek" INTEGER,
	"spend_gbp" INTEGER 
);""",
"""CREATE UNIQUE INDEX "advertiser_weekly_spend_index" ON "advertiser_weekly_spend" (
	"advertiser_id" ASC,
	"week_start_date" ASC
);""",
		]
	}, {
		"filename": "google-political-ads-campaign-targeting.csv",
		"table": "campaign_targeting",
		"sql": [
"""CREATE TABLE "campaign_targeting" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"campaign_id" STRING,
	"age_targeting" STRING,
	"gender_targeting" STRING,
	"geo_targeting_included" STRING,
	"geo_targeting_excluded" STRING,
	"start_date" DATE,
	"end_date" DATE,
	"ads_list" STRING,
	"advertiser_id" STRING,
	"advertiser_name" STRING 
);""",
"""CREATE UNIQUE INDEX "campaign_targeting_index" ON "advertiser_weekly_spend" (
	"advertiser_id" DESC,
	"week_start_date" ASC
);""",
		]
	}, {
		"filename": "google-political-ads-creative-stats.csv",
		"table": "creative_stats",
		"sql": [
"""CREATE TABLE "creative_stats" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"ad_id" STRING,
	"ad_url" STRING,
	"ad_type" STRING,
	"regions" STRING,
	"advertiser_id" STRING,
	"advertiser_name" STRING,
	"ad_campaigns_list" STRING,
	"date_range_start" DATE,
	"date_range_end" DATE,
	"num_of_days" INTEGER,
	"impressions" STRING,
	"spend_usd" STRING,
	"first_served_timestamp" DATE,
	"last_served_timestamp" DATE,
	"spend_range_min_usd" INTEGER,
	"spend_range_max_usd" INTEGER,
	"spend_range_min_eur" INTEGER,
	"spend_range_max_eur" INTEGER,
	"spend_range_min_inr" INTEGER,
	"spend_range_max_inr" INTEGER,
	"spend_range_min_bgn" INTEGER,
	"spend_range_max_bgn" INTEGER,
	"spend_range_min_hrk" INTEGER,
	"spend_range_max_hrk" INTEGER,
	"spend_range_min_czk" INTEGER,
	"spend_range_max_czk" INTEGER,
	"spend_range_min_dkk" INTEGER,
	"spend_range_max_dkk" INTEGER,
	"spend_range_min_huf" INTEGER,
	"spend_range_max_huf" INTEGER,
	"spend_range_min_pln" INTEGER,
	"spend_range_max_pln" INTEGER,
	"spend_range_min_ron" INTEGER,
	"spend_range_max_ron" INTEGER,
	"spend_range_min_sek" INTEGER,
	"spend_range_max_sek" INTEGER,
	"spend_range_min_gbp" INTEGER,
	"spend_range_max_gbp" INTEGER 
);""",
"""CREATE UNIQUE INDEX "creative_stats_index" ON "creative_stats" (
	"ad_id" ASC,
	"advertiser_id" ASC
);""",
		]
	}, {
		"filename": "google-political-ads-geo-spend.csv",
		"table": "geo_spend",
		"sql": [
"""CREATE TABLE "geo_spend" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"country" STRING,
	"country_subdivision_primary" STRING,
	"country_subdivision_secondary" STRING,
	"spend_usd" INTEGER,
	"spend_eur" INTEGER,
	"spend_inr" INTEGER,
	"spend_bgn" INTEGER,
	"spend_hrk" INTEGER,
	"spend_czk" INTEGER,
	"spend_dkk" INTEGER,
	"spend_huf" INTEGER,
	"spend_pln" INTEGER,
	"spend_ron" INTEGER,
	"spend_sek" INTEGER,
	"spend_gbp" INTEGER 
);""",
"""CREATE UNIQUE INDEX "geo_spend_index" ON "geo_spend" (
	"country" ASC,
	"country_subdivision_primary" ASC,
	"country_subdivision_secondary" ASC
);""",
		]
	}, {
		"filename": "google-political-ads-top-keywords-history.csv",
		"table": "top_keywords_history",
		"sql": [
"""CREATE TABLE "top_keywords_history" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"election_cycle" STRING,
	"report_date" DATE,
	"keyword_1" STRING,
	"spend_usd_1" INTEGER,
	"keyword_2" STRING,
	"spend_usd_2" INTEGER,
	"keyword_3" STRING,
	"spend_usd_3" INTEGER,
	"keyword_4" STRING,
	"spend_usd_4" INTEGER,
	"keyword_5" STRING,
	"spend_usd_5" INTEGER,
	"keyword_6" STRING,
	"spend_usd_6" INTEGER,
	"region" STRING,
	"elections" STRING 
);""",
"""CREATE UNIQUE INDEX "top_keywords_history_index" ON "top_keywords_history" (
	"election_cycle" ASC,
	"report_date" ASC
);"""
		]
	}
]


class GoogleAdReportHelper:
	def __init__(self, verbose = True):
		self.verbose = verbose
		self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
		os.makedirs(self._get_download_folder(), exist_ok = True)
		os.makedirs(self._get_export_folder(), exist_ok = True)

	def _get_download_folder(self):
		return os.path.join(Constants.DOWNLOADS_PATH, "google", self.timestamp)

	def _get_download_filename(self):
		return os.path.join(self._get_download_folder(), ZIP_FILENAME)

	def _get_unzipped_folder(self):
		return os.path.join(self._get_download_folder(), CSV_FOLDER)

	def _get_unzipped_csv_filename(self, filename):
		return os.path.join(self._get_download_folder(), CSV_FOLDER, filename)

	def _get_export_folder(self):
		return os.path.join(Constants.EXPORTS_PATH, "google", self.timestamp)

	def _get_export_db_filename(self):
		return os.path.join(self._get_export_folder(), DB_FILENAME)

	def _get_insert_row_sql(self, table, columns):
		table_name = table.split(".")[-1]
		sql_prefix = """INSERT INTO "{}" (""".format(table_name)
		sql_fields = []
		sql_bridge = """) VALUES ("""
		sql_values = []
		sql_suffix = """);"""
		for column in columns:
			sql_fields.append(column)
			sql_values.append("?")
		sql = "\n".join([sql_prefix, ", ".join(sql_fields), sql_bridge, ", ".join(sql_values), sql_suffix])
		return sql

	def download_political_ad_report(self):
		if self.verbose:
			print("[GoogleAdReportHelper] Downloading ad transparency report...")

		filename = self._get_download_filename()
		urllib.request.urlretrieve(URL, filename = filename)

		if self.verbose:
			print("[GoogleAdReportHelper] Downloaded ad transparency report: {:s}".format(filename))

	def unzip_political_ad_report(self):
		if self.verbose:
			print("[GoogleAdReportHelper] Unzipping ad transparency report...")

		filename = self._get_download_filename()
		folder = self._get_download_folder()
		with zipfile.ZipFile(filename) as f:
			f.extractall(folder)

		if self.verbose:
			print("[GoogleAdReportHelper] Unzipped ad transparency report: {:s}".format(folder))

	def export_political_ad_report(self):
		if self.verbose:
			print("[GoogleAdReportHelper] Exporting to SQLite DB...")

		db_filename = self._get_export_db_filename()
		connection = sqlite3.connect(db_filename, detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		connection.row_factory = sqlite3.Row
		cursor = connection.cursor()

		for db_def in DB_DEFS:
			if self.verbose:
				print("[GoogleAdReportHelper] Table: {:s}".format(db_def["table"]))

			for sql in db_def["sql"]:
				cursor.execute(sql)
			filename = self._get_unzipped_csv_filename(db_def["filename"])
			table = db_def["table"]
			with open(filename) as f:
				reader = csv.reader(f)
				is_header = True
				for row in reader:
					if is_header:
						is_header = False
						sql = self._get_insert_row_sql(table, row)
					else:
						cursor.execute(sql, row)

		connection.commit()
		connection.close()
		if self.verbose:
			print("[GoogleAdReportHelper] Exported to SQLite DB: {:s}".format(db_filename))

	def download_and_export(self):
		start_timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		if self.verbose:
			print("[GoogleAdReportHelper] Started at {:s}".format(start_timestamp))

		self.download_political_ad_report()
		self.unzip_political_ad_report()
		self.export_political_ad_report()

		end_timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		if self.verbose:
			print("[GoogleAdReportHelper] Completed at {:s}".format(end_timestamp))
			print()

