#!/usr/bin/env python3

from common import Constants

from datetime import datetime
import dateutil.parser
from glob import glob
import json
import os
import re
import sqlite3

AD_ARCHIVE_ID_REGEX = re.compile(r"^.+?id=(\d+)\&.+$")

# Constants for the ads database
DB_FOLDER = Constants.EXPORTS_PATH
DB_FILENAME = Constants.FACEBOOK_EXPORTS_DB_V1_FILENAME
TABLE_NAME = "all_ads"

# SQL statements
CREATE_ALL_ADS_TABLE_SQL = """CREATE TABLE IF NOT EXISTS "{table}" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"task_key" INTEGER NOT NULL,
	"page_index" INTEGER NOT NULL,
	"page_subindex" INTEGER NOT NULL,

	"ad_creation_time" DATETIME NOT NULL,
	"ad_delivery_start_time" DATETIME,
	"ad_delivery_start_timestamp" REAL,
	"ad_delivery_stop_time" DATETIME,
	"ad_delivery_stop_timestamp" REAL,

	"ad_snapshot_url" TEXT NOT NULL,
	"ad_archive_id" INTEGER NOT NULL,

	"ad_creative_body" TEXT,
	"ad_creative_link_title" TEXT,
	"ad_creative_link_description" TEXT,
	"ad_creative_link_caption" TEXT,

	"page_id" TEXT NOT NULL,
	"page_name" TEXT,
	"funding_entity" TEXT,

	"low_impressions" INTEGER,
	"high_impressions" INTEGER,
	"low_spend" INTEGER,
	"high_spend" INTEGER,
	"currency" TEXT NOT NULL,

	"demographic_distribution" TEXT,
	"region_distribution" TEXT
);""".format(table = TABLE_NAME)

CREATE_ALL_CURRENCIES_TABLE_SQL = """CREATE TABLE IF NOT EXISTS "all_currencies" (
	"key" INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
	"from_currency" TEXT NOT NULL,
	"to_currency" TEXT NOT NULL,
	"multiplier" REAL NOT NULL
);"""

CREATE_ALL_DATES_TABLE_SQL = """CREATE TABLE IF NOT EXISTS "all_dates" (
	"key" INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE,
	"start_date" DATETIME,
	"end_date" DATETIME,
	"label" TEXT
);"""

CREATE_CLEAN_IMPRESSIONS_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_impressions_view AS
	SELECT
		key,
		CASE WHEN high_impressions IS NULL
			THEN low_impressions
			ELSE high_impressions
			END AS high_impressions_capped,
		(high_impressions IS NULL) AS high_impressions_is_capped
	FROM {table}
;""".format(table = TABLE_NAME)

CREATE_CLEAN_SPEND_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_spend_view AS
	SELECT
		key,
		CASE WHEN high_spend IS NULL
			THEN low_spend
			ELSE high_spend
			END AS high_spend_capped,
		(high_spend IS NULL) AS high_spend_is_capped
	FROM {table}
;""".format(table = TABLE_NAME)

CREATE_CLEAN_SPEND_IN_USD_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_spend_in_USD_view AS
	SELECT
		all_ads.key,
		low_spend * multiplier AS low_spend_in_USD,
		high_spend_capped * multiplier AS high_spend_capped_in_USD,
		high_spend_is_capped
	FROM {table}
		INNER JOIN clean_spend_view ON all_ads.key = clean_spend_view.key
		INNER JOIN all_currencies ON all_ads.currency = all_currencies.from_currency
	WHERE all_currencies.to_currency = 'USD'
;""".format(table = TABLE_NAME)

CREATE_CLEAN_SPEND_IN_EUR_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_spend_in_EUR_view AS
	SELECT
		all_ads.key,
		low_spend * multiplier AS low_spend_in_EUR,
		high_spend_capped * multiplier AS high_spend_capped_in_EUR,
		high_spend_is_capped
	FROM {table}
		INNER JOIN clean_spend_view ON all_ads.key = clean_spend_view.key
		INNER JOIN all_currencies ON all_ads.currency = all_currencies.from_currency
	WHERE all_currencies.to_currency = 'EUR'
;""".format(table = TABLE_NAME)

CREATE_CLEAN_SPEND_IN_GBP_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_spend_in_GBP_view AS
	SELECT
		all_ads.key,
		low_spend * multiplier AS low_spend_in_GBP,
		high_spend_capped * multiplier AS high_spend_capped_in_GBP,
		high_spend_is_capped
	FROM {table}
		INNER JOIN clean_spend_view ON all_ads.key = clean_spend_view.key
		INNER JOIN all_currencies ON all_ads.currency = all_currencies.from_currency
	WHERE all_currencies.to_currency = 'GBP'
;""".format(table = TABLE_NAME)

CREATE_CLEAN_AD_DELIVERY_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_ad_delivery_view AS
	SELECT
		key,
		CASE WHEN ((ad_delivery_stop_time IS NULL) OR (ad_delivery_stop_time > download_time))
			THEN download_time
			ELSE ad_delivery_stop_time
			END AS ad_delivery_stop_or_active_time,
		CASE WHEN ((ad_delivery_stop_timestamp IS NULL) OR (ad_delivery_stop_timestamp > download_timestamp))
			THEN download_timestamp
			ELSE ad_delivery_stop_timestamp
			END AS ad_delivery_stop_or_active_timestamp,
		((ad_delivery_stop_timestamp IS NULL) OR (ad_delivery_stop_timestamp > download_timestamp)) AS ad_delivery_is_active
	FROM {table}
	INNER JOIN global_stats
;""".format(table = TABLE_NAME)

CREATE_CLEAN_AD_DELIVERY_DURATION_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_ad_delivery_duration_view AS
	SELECT
		all_ads.key,
		ad_delivery_start_timestamp,
		ad_delivery_stop_or_active_timestamp,
		ad_delivery_is_active,
		ad_delivery_stop_or_active_timestamp - ad_delivery_start_timestamp AS ad_delivery_duration
	FROM {table}
	INNER JOIN clean_ad_delivery_view ON all_ads.key = clean_ad_delivery_view.key
;""".format(table = TABLE_NAME)

CREATE_CLEAN_DATES_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_dates_view AS
	SELECT
		key,
		CASE WHEN start_date IS NULL
			THEN 0
			ELSE JULIANDAY(start_date, 'start of day')
			END AS start_timestamp,
		CASE WHEN end_date IS NULL
			THEN download_timestamp
			ELSE JULIANDAY(end_date, 'start of day', '+1 day')
			END AS end_timestamp,
		label
	FROM all_dates
	INNER JOIN global_stats
;"""

CLEAN_CLEAN_OVERLAPS_VIEW_SQL = """CREATE VIEW IF NOT EXISTS clean_ad_delivery_duration_overlaps_view AS
	SELECT
		ad_key,
		date_key,
		a_timestamp AS start_overlap_timestamp,
		b_timestamp AS end_overlap_timestamp,
		b_timestamp - a_timestamp AS overlap_duration,
		ad_delivery_duration,
		(b_timestamp - a_timestamp) / ad_delivery_duration AS ad_delivery_overlap_fraction
	FROM (
		SELECT
			clean_ad_delivery_duration_view.key AS ad_key,
			clean_dates_view.key AS date_key,
			CASE WHEN ad_delivery_start_timestamp > start_timestamp THEN
				CASE WHEN ad_delivery_start_timestamp < end_timestamp
					THEN ad_delivery_start_timestamp
					ELSE end_timestamp
					END
				ELSE start_timestamp
				END AS a_timestamp,
			CASE WHEN ad_delivery_stop_or_active_timestamp > start_timestamp THEN
				CASE WHEN ad_delivery_stop_or_active_timestamp < end_timestamp
					THEN ad_delivery_stop_or_active_timestamp
					ELSE end_timestamp
					END
				ELSE start_timestamp
				END AS b_timestamp,
			ad_delivery_duration
		FROM clean_ad_delivery_duration_view
		INNER JOIN clean_dates_view
	)
	WHERE b_timestamp - a_timestamp > 0
	ORDER BY date_key ASC, ad_delivery_overlap_fraction DESC
;"""

CREATE_ADVERTISER_REPORT_SQL = """CREATE VIEW IF NOT EXISTS advertiser_report AS
	SELECT
		all_ads.page_id,
		page_name,
		funding_entities,
		COUNT(*) AS total_ads,
		ROUND(SUM(low_spend_in_GBP), 2) AS total_low_spend_in_GBP,
		ROUND(SUM(high_spend_capped_in_GBP), 2) AS total_high_spend_capped_in_GBP,
		SUM(high_spend_is_capped) > 0 AS total_high_spend_is_capped_in_GBP,
		SUM(low_impressions) AS total_low_impressions,
		SUM(high_impressions_capped) AS total_high_impressions_capped,
		SUM(high_impressions_is_capped) > 0 AS total_high_impressions_is_capped
	FROM all_ads
	INNER JOIN advertiser_funding_entities_table ON all_ads.page_id = advertiser_funding_entities_table.page_id
	INNER JOIN clean_spend_in_GBP_view ON all_ads.key = clean_spend_in_GBP_view.key
	INNER JOIN clean_impressions_view ON all_ads.key = clean_impressions_view.key
	GROUP BY all_ads.page_id
	ORDER BY total_high_impressions_capped DESC, total_high_impressions_is_capped DESC, total_low_impressions DESC
;"""

CREATE_ADVERTISER_REPORT_BY_DATES_SQL = """CREATE VIEW IF NOT EXISTS advertiser_report_by_dates AS
	SELECT
		all_ads.page_id,
		page_name,
		funding_entities,
		date_key,
		all_dates.label AS date_label,
		COUNT(*) AS total_ads,
		ROUND(SUM(low_spend_in_GBP * ad_delivery_overlap_fraction), 2) AS total_low_spend_in_GBP,
		ROUND(SUM(high_spend_capped_in_GBP * ad_delivery_overlap_fraction), 2) AS total_high_spend_capped_in_GBP,
		SUM(high_spend_is_capped) > 0 AS total_high_spend_is_capped_in_GBP,
		ROUND(SUM(low_impressions * ad_delivery_overlap_fraction), 1) AS total_low_impressions,
		ROUND(SUM(high_impressions_capped * ad_delivery_overlap_fraction), 1) AS total_high_impressions_capped,
		SUM(high_impressions_is_capped) > 0 AS total_high_impressions_is_capped
	FROM all_ads
	INNER JOIN advertiser_funding_entities_table ON all_ads.page_id = advertiser_funding_entities_table.page_id
	INNER JOIN clean_spend_in_GBP_view ON all_ads.key = clean_spend_in_GBP_view.key
	INNER JOIN clean_impressions_view ON all_ads.key = clean_impressions_view.key
	INNER JOIN clean_ad_delivery_duration_overlaps_view ON all_ads.key = clean_ad_delivery_duration_overlaps_view.ad_key
	INNER JOIN all_dates ON date_key = all_dates.key
	GROUP BY all_ads.page_id, date_key
	ORDER BY all_ads.page_name, date_key
;"""

INSERT_ALL_ADS_TABLE_SQL = """REPLACE INTO "{table}" (
	"task_key", "page_index", "page_subindex",
	"ad_creation_time", "ad_delivery_start_time", "ad_delivery_start_timestamp", "ad_delivery_stop_time", "ad_delivery_stop_timestamp",
	"ad_snapshot_url", "ad_archive_id",
	"ad_creative_body", "ad_creative_link_title", "ad_creative_link_description", "ad_creative_link_caption",
	"page_id", "page_name", "funding_entity",
	"low_impressions", "high_impressions", "low_spend", "high_spend", "currency",
	"demographic_distribution",
	"region_distribution"
) VALUES (
	?, ?, ?,
	?, ?, JULIANDAY(?), ?, JULIANDAY(?),
	?, ?,
	?, ?, ?, ?,
	?, ?, ?,
	?, ?, ?, ?, ?,
	?,
	?
);""".format(table = TABLE_NAME)

INSERT_ALL_CURRENCIES_TABLE_SQL = """INSERT OR IGNORE INTO all_currencies (
	from_currency, to_currency, multiplier
) VALUES (
	?, ?, ?
);"""

INSERT_ALL_DATES_TABLE_SQL = """INSERT OR IGNORE INTO all_dates (
	start_date, end_date, label
) VALUES (
	?, ?, ?
);"""

CREATE_AD_ARCHIVE_ID_INDEX_SQL = """CREATE UNIQUE INDEX IF NOT EXISTS {table}__ad_archive_id__index ON {table} (ad_archive_id ASC);""".format(table = TABLE_NAME)
CREATE_PAGE_ID_INDEX_SQL = """CREATE INDEX IF NOT EXISTS {table}__page_id__index ON {table} (page_id ASC);""".format(table = TABLE_NAME)

CREATE_FROM_CURRENCY_INDEX_SQL = """CREATE UNIQUE INDEX IF NOT EXISTS all_currencies__index ON all_currencies (to_currency ASC, from_currency ASC);"""

CREATE_ALL_LOG_FILES_TABLE_SQL = """CREATE TABLE IF NOT EXISTS "all_log_files" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"log_file" TEXT NOT NULL UNIQUE
);"""

SELECT_ALL_LOG_FILES_SQL = """SELECT log_file FROM all_log_files;"""
INSERT_LOG_FILE_SQL = """INSERT OR IGNORE INTO all_log_files (
	log_file
) VALUES (
	?
)""";

TABLE_EXISTS_SQL = """SELECT COUNT(*) = 1 FROM "sqlite_master" WHERE "type" = "table" AND "name" = "{table}";""".format(table = TABLE_NAME)

POST_PROCESSING_SQL = [
"""DROP TABLE IF EXISTS advertiser_funding_entity_table;
""",
"""DROP INDEX IF EXISTS advertiser_funding_entity_table__index;
""",
"""CREATE TABLE advertiser_funding_entity_table AS
	SELECT DISTINCT page_id, funding_entity
	FROM all_ads
	ORDER BY page_id, funding_entity;
""",
"""CREATE INDEX advertiser_funding_entity_table__index ON advertiser_funding_entity_table (page_id ASC);
""",
"""DROP TABLE IF EXISTS advertiser_funding_entities_table;
""",
"""DROP INDEX IF EXISTS advertiser_funding_entities_table__index;
""",
"""CREATE TABLE advertiser_funding_entities_table AS
	SELECT page_id, GROUP_CONCAT(funding_entity, " || ") AS funding_entities
	FROM advertiser_funding_entity_table
	GROUP BY page_id
	ORDER BY page_id;
""",
"""CREATE UNIQUE INDEX advertiser_funding_entities_table__index ON advertiser_funding_entities_table (page_id ASC);
""",
"""DROP TABLE IF EXISTS global_stats;
""",
"""CREATE TABLE IF NOT EXISTS global_stats AS
	SELECT
		CURRENT_TIMESTAMP AS download_time,
		JULIANDAY(CURRENT_TIMESTAMP) AS download_timestamp;
""",
]

class ExportsDBv1:
	def __init__(self, experiment_key, db_folder = None, verbose = True, is_cumulative = True):
		assert isinstance(experiment_key, str)
		assert isinstance(verbose, bool)
		self.experiment_key = experiment_key
		self.verbose = verbose
		self.is_cumulative = is_cumulative
		if self.is_cumulative:
			self.db_folder = os.path.join(DB_FOLDER if db_folder is None else db_folder, self.experiment_key)
		else:
			now = datetime.now()
			timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
			self.db_folder = os.path.join(DB_FOLDER if db_folder is None else db_folder, self.experiment_key, timestamp)
		self.db_path = os.path.join(self.db_folder, DB_FILENAME)
		self.connection = None
		self.cursor = None
		self._init_db_folder()

	def _init_db_folder(self):
		os.makedirs(self.db_folder, exist_ok = True)

	def open(self):
		if self.verbose:
			print()
			print("[ExportsDB v1.0] Connecting to database...")
		self.connection = sqlite3.connect(self.db_path, detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self.connection.row_factory = sqlite3.Row
		self.cursor = self.connection.cursor()

		if not self._has_tables():
			self._create_tables()
			self._create_indexes()
			self._create_views()

	def close(self):
		self._post_process()

		if self.verbose:
			print("[ExportsDB v1.0] Committing changes to database...")
		self.connection.commit()

		if self.verbose:
			print("[ExportsDB v1.0] Disconnect from database...")
			print()
		self.connection.close()

	def _has_tables(self):
		self.cursor.execute(TABLE_EXISTS_SQL)
		one_row = self.cursor.fetchone()
		table_exists = bool(one_row[0])
		return table_exists

	def _create_tables(self):
		if self.verbose:
			print("[ExportsDB v1.0] Creating table '{}'...".format(TABLE_NAME))
		print(CREATE_ALL_ADS_TABLE_SQL)
		self.cursor.execute(CREATE_ALL_ADS_TABLE_SQL)
		print(CREATE_ALL_CURRENCIES_TABLE_SQL)
		self.cursor.execute(CREATE_ALL_CURRENCIES_TABLE_SQL)
		print(CREATE_ALL_DATES_TABLE_SQL)
		self.cursor.execute(CREATE_ALL_DATES_TABLE_SQL)
		print(CREATE_ALL_LOG_FILES_TABLE_SQL)
		self.cursor.execute(CREATE_ALL_LOG_FILES_TABLE_SQL)

	def _create_indexes(self):
		if self.verbose:
			print("[ExportsDB v1.0] Creating indexes...")
		print(CREATE_AD_ARCHIVE_ID_INDEX_SQL)
		self.cursor.execute(CREATE_AD_ARCHIVE_ID_INDEX_SQL)
		print(CREATE_PAGE_ID_INDEX_SQL)
		self.cursor.execute(CREATE_PAGE_ID_INDEX_SQL)
		print(CREATE_FROM_CURRENCY_INDEX_SQL)
		self.cursor.execute(CREATE_FROM_CURRENCY_INDEX_SQL)

	def _create_views(self):
		if self.verbose:
			print("[ExportsDB v1.0] Creating views...")
		print(CREATE_CLEAN_IMPRESSIONS_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_IMPRESSIONS_VIEW_SQL)
		print(CREATE_CLEAN_SPEND_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_SPEND_VIEW_SQL)
		print(CREATE_CLEAN_SPEND_IN_USD_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_SPEND_IN_USD_VIEW_SQL)
		print(CREATE_CLEAN_SPEND_IN_EUR_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_SPEND_IN_EUR_VIEW_SQL)
		print(CREATE_CLEAN_SPEND_IN_GBP_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_SPEND_IN_GBP_VIEW_SQL)
		print(CREATE_CLEAN_AD_DELIVERY_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_AD_DELIVERY_VIEW_SQL)
		print(CREATE_CLEAN_AD_DELIVERY_DURATION_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_AD_DELIVERY_DURATION_VIEW_SQL)
		print(CREATE_CLEAN_DATES_VIEW_SQL)
		self.cursor.execute(CREATE_CLEAN_DATES_VIEW_SQL)
		print(CLEAN_CLEAN_OVERLAPS_VIEW_SQL)
		self.cursor.execute(CLEAN_CLEAN_OVERLAPS_VIEW_SQL)

		print(CREATE_ADVERTISER_REPORT_SQL)
		self.cursor.execute(CREATE_ADVERTISER_REPORT_SQL)
		print(CREATE_ADVERTISER_REPORT_BY_DATES_SQL)
		self.cursor.execute(CREATE_ADVERTISER_REPORT_BY_DATES_SQL)

	def _post_process(self):
		if self.verbose:
			print("[ExportsDB v1.0] Post processing...")
		for sql in POST_PROCESSING_SQL:
			print(sql)
			self.cursor.execute(sql)

	def _serialize_json(self, text):
		return json.dumps(text, separators = (",", ":"))

	def _deserialize_json(self, blob):
		return json.loads(blob)

	def _get_all_log_files(self):
		self.cursor.execute(SELECT_ALL_LOG_FILES_SQL)
		rows = self.cursor.fetchall()
		return [row[0] for row in rows]

	def _insert_log_file(self, filename):
		self.cursor.execute(INSERT_LOG_FILE_SQL, (filename, ))

	def _insert_ads(self, task_key, page_index, page_subindex, ad):
		assert isinstance(task_key, int)
		assert isinstance(page_index, int)
		assert isinstance(page_subindex, int)

		ad_creation_time = dateutil.parser.parse(ad["ad_creation_time"])
		ad_delivery_start_time = dateutil.parser.parse(ad["ad_delivery_start_time"]) if "ad_delivery_start_time" in ad else None
		ad_delivery_stop_time = dateutil.parser.parse(ad["ad_delivery_stop_time"]) if "ad_delivery_stop_time" in ad else None
		assert isinstance(ad_creation_time, datetime)
		assert isinstance(ad_delivery_start_time, datetime) or ad_delivery_start_time is None
		assert isinstance(ad_delivery_stop_time, datetime) or ad_delivery_stop_time is None

		ad_snapshot_url = ad["ad_snapshot_url"]
		ad_archive_id = int(AD_ARCHIVE_ID_REGEX.search(ad_snapshot_url).group(1))
		assert isinstance(ad_snapshot_url, str)
		assert isinstance(ad_archive_id, int)
		assert str(ad_archive_id) == AD_ARCHIVE_ID_REGEX.search(ad_snapshot_url).group(1)

		ad_creative_body = ad["ad_creative_body"] if "ad_creative_body" in ad else None
		ad_creative_link_title = ad["ad_creative_link_title"] if "ad_creative_link_title" in ad else None
		ad_creative_link_description = ad["ad_creative_link_description"] if "ad_creative_link_description" in ad else None
		ad_creative_link_caption = ad["ad_creative_link_caption"] if "ad_creative_link_caption" in ad else None
		assert isinstance(ad_creative_body, str) or ad_creative_body is None
		assert isinstance(ad_creative_link_title, str) or ad_creative_link_title is None
		assert isinstance(ad_creative_link_description, str) or ad_creative_link_description is None
		assert isinstance(ad_creative_link_caption, str) or ad_creative_link_caption is None

		page_id = ad["page_id"]
		page_name = ad["page_name"] if "page_name" in ad else None
		funding_entity = ad["funding_entity"] if "funding_entity" in ad else None
		assert isinstance(page_id, str)
		assert page_name is None or isinstance(page_name, str)
		assert funding_entity is None or isinstance(funding_entity, str)

		low_impressions = int(ad["impressions"]["lower_bound"]) if "impressions" in ad and "lower_bound" in ad["impressions"] else None
		high_impressions = int(ad["impressions"]["upper_bound"]) if "impressions" in ad and "upper_bound" in ad["impressions"] else None
		low_spend = int(ad["spend"]["lower_bound"]) if "spend" in ad and "lower_bound" in ad["spend"] else None
		high_spend = int(ad["spend"]["upper_bound"]) if "spend" in ad and "upper_bound" in ad["spend"] else None
		currency = ad["currency"]
		assert isinstance(low_impressions, int) or low_impressions is None
		assert isinstance(high_impressions, int) or high_impressions is None
		assert isinstance(low_spend, int) or low_spend is None
		assert isinstance(high_spend, int) or high_spend is None
		assert isinstance(currency, str)

		demographic_distribution_str = self._serialize_json(ad["demographic_distribution"]) if "demographic_distribution" in ad else None
		region_distribution_str = self._serialize_json(ad["region_distribution"]) if "region_distribution" in ad else None

		self.cursor.execute(INSERT_ALL_ADS_TABLE_SQL, (
			task_key, page_index, page_subindex,
			ad_creation_time, ad_delivery_start_time, ad_delivery_start_time, ad_delivery_stop_time, ad_delivery_stop_time,
			ad_snapshot_url, ad_archive_id,
			ad_creative_body, ad_creative_link_title, ad_creative_link_description, ad_creative_link_caption,
			page_id, page_name, funding_entity,
			low_impressions, high_impressions, low_spend, high_spend, currency,
			demographic_distribution_str,
			region_distribution_str,
		))

	def insert_currencies(self):
		with open(os.path.join("..", "external_files", "currencies.json")) as f:
			all_data = json.load(f)
		to_currencies = ["USD", "EUR", "GBP"]
		for to_currency in to_currencies:
			data = all_data[to_currency]
			field = "to_{}".format(to_currency)
			for d in data:
				from_currency = d["code"]
				multiplier = d[field]
				self.cursor.execute(INSERT_ALL_CURRENCIES_TABLE_SQL, (from_currency, to_currency, multiplier, ))
		self.connection.commit()

	def insert_dates(self):
		self.cursor.execute(INSERT_ALL_DATES_TABLE_SQL, (None, None, "All ads", ))
		self.cursor.execute(INSERT_ALL_DATES_TABLE_SQL, (None, "2019-08-31", "Aug 2019 and earlier", ))
		self.cursor.execute(INSERT_ALL_DATES_TABLE_SQL, ("2019-09-01", None, "Sep 2019 and later", ))
		self.connection.commit()

	def export_all_ads(self):
		self.open()
		self.insert_currencies()
		self.insert_dates()
		existing_filenames = frozenset(self._get_all_log_files())

		folder = os.path.join(Constants.EXPORTS_PATH, "data")
		task_key_regex = re.compile(r"task\-0+(\d+)\.json")
		page_index = 0
		glob_pattern = "{}/facebook/{}/*/task-*.json".format(Constants.DOWNLOADS_PATH, self.experiment_key)
		filenames = glob(glob_pattern)
		filenames.sort()
		for filename in filenames:
			if filename in existing_filenames:
				print("skipping file: ", filename)
			else:
				print("exporting file: ", filename)
				task_key = int(task_key_regex.search(filename).group(1))
				with open(filename) as f:
					response = json.load(f)
					if "data" in response:
						data = response["data"]
						for page_subindex, d in enumerate(data):
							self._insert_ads(task_key, page_index, page_subindex, d)
						page_index += 1
				self._insert_log_file(filename)
		self.close()
