#!/usr/bin/env python3

from common import Constants

from datetime import datetime
import json
import os
import sqlite3

# Constants for the ads database
DB_FOLDER = Constants.DOWNLOADS_PATH
DB_FILENAME = Constants.FACEBOOK_DOWNLOADS_DB_FILENAME
TABLE_NAME = "all_tasks_table"

# SQL statements
CREATE_TABLE_SQL = """CREATE TABLE "{table}" (
	"key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"task_key" INTEGER NOT NULL,
	"task_priority" INTEGER NOT NULL,
	"creation_timestamp" DATETIME NOT NULL,
	"start_timestamp" DATETIME NOT NULL,
	"finish_timestamp" DATETIME NOT NULL,
	"experiment_key" TEXT NOT NULL,
	"split_index" INTEGER NOT NULL,
	"page_index" INTEGER NOT NULL,
	"page_attempt" INTEGER NOT NULL,
	"attempt_index" INTEGER NOT NULL,
	"experiment_spec" TEXT NOT NULL,
	"split_spec" TEXT NOT NULL,
	"page_spec" TEXT NOT NULL,
	"attempt_spec" TEXT NOT NULL,
	"continuation" TEXT NOT NULL,
	"finish_code" INTEGER NOT NULL,
	"finish_log" TEXT NOT NULL,
	"access_token" TEXT NOT NULL,
	"ad_count" INTEGER NOT NULL,
	"paging_cursor" TEXT,
	"error_code" INTEGER,
	"request_url" TEXT NOT NULL,
	"request_timestamp" DATETIME NOT NULL,
	"response_timestamp" DATETIME,
	"duration" REAL,
	"response_header" TEXT NOT NULL,
	"response_body_filename" TEXT,
	"response_body_length" INTEGER,
	"response_html_filename" TEXT,
	"response_html_length" INTEGER,
	"response_error" TEXT
);""".format(table = TABLE_NAME)

INSERT_TASK_SQL = """INSERT INTO "{table}" (
	"task_key", "task_priority",
	"creation_timestamp", "start_timestamp", "finish_timestamp",
	"experiment_key", "split_index", "page_index", "page_attempt", "attempt_index",
	"experiment_spec", "split_spec", "page_spec", "attempt_spec",
	"continuation",
	"finish_code", "finish_log",
	"access_token", "ad_count", "paging_cursor", "error_code",
	"request_url",
	"request_timestamp", "response_timestamp", "duration",
	"response_header", "response_body_filename", "response_body_length", "response_html_filename", "response_html_length", "response_error"
) VALUES (
	?, ?,
	?, ?, ?,
	?, ?, ?, ?, ?,
	?, ?, ?, ?,
	?,
	?, ?,
	?, ?, ?, ?,
	?,
	?, ?, ?,
	?, ?, ?, ?, ?, ?
);""".format(table = TABLE_NAME)

TABLE_EXISTS_SQL = """SELECT COUNT(*) = 1 FROM "sqlite_master" WHERE "type" = "table" AND "name" = "{table}";""".format(table = TABLE_NAME)

class DownloadsDB:
	def __init__(self, db_folder = None, verbose = True):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self.db_folder = DB_FOLDER if db_folder is None else db_folder
		self.db_path = os.path.join(self.db_folder, DB_FILENAME)
		self.connection = None
		self.cursor = None
		self._init_db_folder()

	def _init_db_folder(self):
		os.makedirs(self.db_folder, exist_ok = True)

	def open(self):
		if self.verbose:
			print()
			print("[DownloadsDB] Connecting to database...")
		self.connection = sqlite3.connect(self.db_path, detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self.connection.row_factory = sqlite3.Row
		self.cursor = self.connection.cursor()

		if not self._has_tables():
			self._create_tables()

	def close(self):
		if self.verbose:
			print("[DownloadsDB] Committing changes to database...")
		self.connection.commit()

		if self.verbose:
			print("[DownloadsDB] Disconnect from database...")
			print()
		self.connection.close()

	def _has_tables(self):
		self.cursor.execute(TABLE_EXISTS_SQL)
		one_row = self.cursor.fetchone()
		table_exists = bool(one_row[0])
		return table_exists

	def _create_tables(self):
		if self.verbose or verbose:
			print("[DownloadsDB] Creating table '{}'...".format(TABLE_NAME))
		print(CREATE_TABLE_SQL)
		self.cursor.execute(CREATE_TABLE_SQL)

	def _serialize_json(self, text):
		return json.dumps(text, indent = 2, sort_keys = True)

	def _deserialize_json(self, blob):
		return json.loads(blob)

	def insert(self, task_as_dict, url, response):
		task_key = task_as_dict["task_key"]
		task_priority = task_as_dict["task_priority"]

		creation_timestamp = task_as_dict["creation_timestamp"]
		start_timestamp = task_as_dict["start_timestamp"]
		finish_timestamp = task_as_dict["finish_timestamp"]

		experiment_key = task_as_dict["experiment_key"]
		split_index = task_as_dict["split_index"]
		page_index = task_as_dict["page_index"]
		page_attempt = task_as_dict["page_attempt"]
		attempt_index = task_as_dict["attempt_index"]

		experiment_spec = task_as_dict["experiment_spec"]
		split_spec = task_as_dict["split_spec"]
		page_spec = task_as_dict["page_spec"]
		attempt_spec = task_as_dict["attempt_spec"]

		continuation = task_as_dict["continuation"]

		finish_code = task_as_dict["finish_code"]
		finish_log = task_as_dict["finish_log"]

		access_token = task_as_dict["access_token"]
		ad_count = task_as_dict["ad_count"]
		paging_cursor = task_as_dict["paging_cursor"]
		error_code = task_as_dict["error_code"]
		
		experiment_folder = task_as_dict["experiment_folder"]
		data_path = os.path.join(self.db_folder, experiment_folder)
		os.makedirs(data_path, exist_ok = True)

		request_url = url
		
		request_timestamp = response["request_timestamp"]
		response_timestamp = response["response_timestamp"]
		duration = response["duration"]
		response_header_str = self._serialize_json(response["response_header"])
		response_body = response["response_body"]
		if response_body is not None:
			response_body_filename = "{:s}/task-{:06d}.json".format(data_path, task_key)
			response_body_str = json.dumps(response_body, indent = 2, sort_keys = True)
			response_body_length = len(response_body_str)
			with open(response_body_filename, "w") as f:
				f.write(response_body_str)
		else:
			response_body_filename = None
			response_body_length = None
		response_html = response["response_html"]
		if response_html is not None:
			response_html_filename = "{:s}/task-{:06d}.html".format(data_path, task_key)
			response_html_length = len(response_html)
			with open(response_html_filename, "w") as f:
				f.write(response_html)
		else:
			response_html_filename = None
			response_html_length = None
		response_error = response["response_error"]
		
		self.cursor.execute(INSERT_TASK_SQL, (
			task_key, task_priority,
			creation_timestamp, start_timestamp, finish_timestamp,
			experiment_key, split_index, page_index, page_attempt, attempt_index,
			experiment_spec, split_spec, page_spec, attempt_spec,
			continuation,
			finish_code, finish_log,
			access_token, ad_count, paging_cursor, error_code,
			request_url,
			request_timestamp, response_timestamp, duration,
			response_header_str, response_body_filename, response_body_length, response_html_filename, response_html_length, response_error,
		))
