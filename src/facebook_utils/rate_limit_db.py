#!/usr/bin/env python3

from common import Constants

from datetime import datetime, timedelta
import os
import sqlite3
from typing import NamedTuple

# Constants for the rate limit database
DB_FOLDER = Constants.DB_PATH
DB_FILENAME = "rate_limit.sqlite"
TABLE_NAME = "request_timestamps"

# SQL statements
CREATE_TABLE_SQL = """CREATE TABLE "{table}" (
	"timestamp" DATETIME NOT NULL
);""".format(table = TABLE_NAME)

CREATE_INDEX_SQL = """CREATE INDEX "timestamp_index" ON "{table}" (
	"timestamp" ASC
);""".format(table = TABLE_NAME)

CREATE_VIEW_SQL = """CREATE VIEW """

TABLE_EXISTS_SQL = """SELECT COUNT(*) = 1 FROM "sqlite_master" WHERE "type" = "table" AND "name" = "{table}";""".format(table = TABLE_NAME)

SELECT_TIMESTAMPS_SQL = """SELECT "timestamp" AS "[timestamp]" FROM "{table}" WHERE "timestamp" > ? ORDER BY "timestamp" ASC;""".format(table = TABLE_NAME)

INSERT_TIMESTAMP_SQL = """INSERT INTO "{table}" VALUES (?);""".format(table = TABLE_NAME)

DEFAULT_DURATION_MINUTES = 15
DEFAULT_DURATION_SECONDS = 0

# Custom data types
class UsageData(NamedTuple):
	count: int
	duration: float

class RateLimitDB:
	def __init__(self, verbose = True, db_folder = None):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self.db_folder = DB_FOLDER if db_folder is None else db_folder
		self.db_path = os.path.join(self.db_folder, DB_FILENAME)
		self.connection = None
		self.cursor = None
		self._init_folder()

	def _init_folder(self):
		os.makedirs(self.db_folder, exist_ok = True)

	def _has_tables(self) -> bool:
		self.cursor.execute(TABLE_EXISTS_SQL)
		one_row = self.cursor.fetchone()
		table_exists = bool(one_row[0])
		return table_exists

	def _create_tables(self):
		if self.verbose:
			print("[RateLimitDB] Creating table '{}'...".format(TABLE_NAME))
			print(CREATE_TABLE_SQL)
		self.cursor.execute(CREATE_TABLE_SQL)

	def _create_indexes(self):
		if self.verbose:
			print("[RateLimitDB] Creating indexes...")
			print(CREATE_INDEX_SQL)
		self.cursor.execute(CREATE_INDEX_SQL)

	def open(self):
		if self.verbose:
			print()
			print("[RateLimitDB] Connecting to database...")
		self.connection = sqlite3.connect(self.db_path, detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self.connection.row_factory = sqlite3.Row
		self.cursor = self.connection.cursor()
		if not self._has_tables():
			self._create_tables()
			self._create_indexes()

	def close(self):
		if self.verbose:
			print("[RateLimitDB] Committing changes to database...")
		self.connection.commit()
		if self.verbose:
			print("[RateLimitDB] Disconnecting from database...")
			print()
		self.connection.close()

	def add_timestamp(self):
		timestamp = datetime.now()
		if self.verbose:
			print("[RateLimitDB] Adding a timestamp...")
			print("    Timestamp = {:s}".format(timestamp.strftime("%Y-%m-%d %H:%M:%S")))
		self.cursor.execute(INSERT_TIMESTAMP_SQL, (timestamp, ))

	def check_usage(self, duration = timedelta(minutes = DEFAULT_DURATION_MINUTES, seconds = DEFAULT_DURATION_SECONDS)):
		assert isinstance(duration, timedelta)
		end_timestamp = datetime.now()
		start_timestamp = end_timestamp - duration
		if self.verbose:
			print("[RateLimitDB] Checking rate limit...")
		self.cursor.execute(SELECT_TIMESTAMPS_SQL, (start_timestamp, ))
		all_rows = self.cursor.fetchall()
		all_timestamps = [row[0] for row in all_rows]
		if len(all_timestamps) == 0:
			prior_request_count = 0
			prior_request_duration = 0.0
		else:
			prior_request_count = len(all_timestamps)
			prior_request_duration = (end_timestamp - all_timestamps[0]).total_seconds()
		usage_data = UsageData(prior_request_count, prior_request_duration)
		if self.verbose:
			print("    Found {:d} timestamps in the past {:.3f} seconds".format(usage_data.count, usage_data.duration))
		return usage_data
