#!/usr/bin/env python3

from common import Constants

import configparser
import json
import os
import sqlite3

# Constants for the queue database
DB_FOLDER = Constants.DB_PATH
DB_FILENAME = Constants.QUEUE_DB_FILENAME

# SQL database constants
TABLE_NAME = "all_tasks_table"
ACTIVE_TASKS = "active_tasks"
QUEUED_TASKS = "queued_tasks"
STARTED_TASKS = "started_tasks"
FINISHED_TASKS = "finished_tasks"
FAILED_TASKS = "failed_tasks"
CANCELLED_TASKS = "cancelled_tasks"
ACTIVE_TASK_COUNT = "active_task_count"
NEXT_ACTIVE_TASK = "next_active_task"
ANY_TASK = "any_task"

# CANCELLED - (*, *, *, 1)
#   Any normal task can be manually cancelled by setting cancelled = 1.
#   Any cancelled task can be restarted by setting cancelled = 0.
#
# QUEUED    - (0, 0, *, 0)
# STARTED   - (1, 0, *, 0)
# FINISHED  - (1, 1, *, 0)
#           - (0, 1, *, 0) should not exist
#   Normal tasks can be queued, started, or finished.
#   Active tasks are queued tasks sorted in descending priority.
#
# FAILED    - (*, *, 1, 1)
#           - (*, *, 1, 0) are restarted experiments.
#   Final state of a failed experiment.
#   By default, failed tasks are also cancelled (*, *, 1, 1).
#   A failed task can be turned into a normal task by setting cancelled = 0.
#   Failed tasks that are re-started have a status of (*, *, 1, 0)

# SQL statements
CREATE_TABLE_SQL = """CREATE TABLE "{table}" (
	"task_key" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"task_priority" INTEGER NOT NULL,
	"is_task_started" BOOLEAN NOT NULL DEFAULT 0,
	"is_task_finished" BOOLEAN NOT NULL DEFAULT 0,
	"is_task_cancelled" BOOLEAN NOT NULL DEFAULT 0,
	"is_task_failed" BOOLEAN NOT NULL DEFAULT 0,
	"creation_timestamp" DATETIME NOT NULL DEFAULT (DATETIME('NOW', 'LOCALTIME')),
	"start_timestamp" DATETIME DEFAULT NULL,
	"finish_timestamp" DATETIME DEFAULT NULL,
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
	"finish_code" INTEGER DEFAULT NULL,
	"finish_log" TEXT DEFAULT NULL,
	"access_token" TEXT DEFAULT NULL,
	"ad_count" INTEGER DEFAULT NULL,
	"paging_cursor" TEXT DEFAULT NULL,
	"error_code" INTEGER DEFAULT NULL,
	"experiment_folder" TEXT NOT NULL
);""".format(table = TABLE_NAME)

INSERT_CREATE_NORMAL_TASK_SQL = """INSERT INTO {table} (
	creation_timestamp,
	task_priority,
	experiment_key, split_index, page_index, page_attempt, attempt_index,
	experiment_spec, split_spec, page_spec, attempt_spec,
	continuation,
	experiment_folder
) VALUES (
	DATETIME('NOW', 'LOCALTIME'),
	?,
	?, ?, ?, ?, ?,
	?, ?, ?, ?,
	?,
	?
);""".format(table = TABLE_NAME)

INSERT_CREATE_FAILED_TASK_SQL = """INSERT INTO {table} (
	creation_timestamp,
	task_priority, is_task_failed,
	experiment_key, split_index, page_index, page_attempt, attempt_index,
	experiment_spec, split_spec, page_spec, attempt_spec,
	continuation,
	experiment_folder
) VALUES (
	DATETIME('NOW', 'LOCALTIME'),
	?, 1,
	?, ?, ?, ?, ?,
	?, ?, ?, ?,
	?,
	?
);""".format(table = TABLE_NAME)

UPDATE_START_TASK_SQL = """UPDATE {table} SET
	is_task_started = 1,
	start_timestamp = (DATETIME('NOW', 'LOCALTIME'))
WHERE task_key = ? AND is_task_cancelled = 0 AND is_task_started = 0 AND is_task_finished = 0
;""".format(table = TABLE_NAME)

UPDATE_FINISH_TASK_SQL = """UPDATE {table} SET
	is_task_finished = 1,
	finish_timestamp = (DATETIME('NOW', 'LOCALTIME'))
WHERE task_key = ? AND is_task_cancelled = 0 AND is_task_started = 1 AND is_task_finished = 0
;""".format(table = TABLE_NAME)

UPDATE_CANCEL_TASK_SQL = """UPDATE {table} SET
	is_task_cancelled = 1
where task_key = ?
;""".format(table = TABLE_NAME)

UPDATE_RESTART_TASK_SQL = """UPDATE {table} SET
	is_task_started = 0,
	is_task_finished = 0,
	is_task_cancelled = 0
where task_key = ?
;""".format(table = TABLE_NAME)

UPDATE_AMEND_TASK_SQL = """UPDATE {table} SET
	finish_code = ?,
	finish_log = ?,
	access_token = ?,
	ad_count = ?,
	paging_cursor = ?,
	error_code = ?
WHERE task_key = ?
;""".format(table = TABLE_NAME)

CREATE_ACTIVE_TASKS_INDEX_SQL = """CREATE INDEX {view}_index ON {table} (
	is_task_cancelled ASC,
	is_task_started ASC,
	is_task_finished ASC,
	task_priority DESC,
	creation_timestamp DESC
);""".format(table = TABLE_NAME, view = ACTIVE_TASKS)

CREATE_CANCELLED_TASKS_INDEX_SQL = """CREATE INDEX {view}_index ON {table} (
	is_task_cancelled DESC,
	creation_timestamp DESC
);""".format(table = TABLE_NAME, view = CANCELLED_TASKS)

CREATE_QUEUED_TASKS_INDEX_SQL =  """CREATE INDEX {view}_index ON {table} (
	is_task_cancelled ASC,
	is_task_started ASC,
	is_task_finished ASC,
	creation_timestamp DESC
);""".format(table = TABLE_NAME, view = QUEUED_TASKS)

CREATE_STARTED_TASKS_INDEX_SQL = """CREATE INDEX {view}_index on {table} (
	is_task_cancelled ASC,
	is_task_started DESC,
	is_task_finished ASC,
	start_timestamp DESC
);""".format(table = TABLE_NAME, view = STARTED_TASKS)

CREATE_FINISHED_TASKS_INDEX_SQL = """CREATE INDEX {view}_index ON {table} (
	is_task_cancelled ASC,
	is_task_started DESC,
	is_task_finished DESC,
	finish_timestamp DESC
);""".format(table = TABLE_NAME, view = FINISHED_TASKS)

CREATE_FAILED_TASKS_INDEX_SQL = """CREATE INDEX {view}_index ON {table} (
	is_task_failed DESC,
	creation_timestamp DESC
);""".format(table = TABLE_NAME, view = FAILED_TASKS)

CREATE_ACTIVE_TASKS_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_cancelled = 0 AND is_task_started = 0 AND is_task_finished = 0
	ORDER BY task_priority DESC, creation_timestamp DESC
;""".format(table = TABLE_NAME, view = ACTIVE_TASKS)

CREATE_CANCELLED_TASKS_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_cancelled = 1
	ORDER BY creation_timestamp DESC
;""".format(table = TABLE_NAME, view = CANCELLED_TASKS)

CREATE_QUEUED_TASKS_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_cancelled = 0 AND is_task_started = 0 AND is_task_finished = 0
	ORDER BY creation_timestamp DESC
;""".format(table = TABLE_NAME, view = QUEUED_TASKS)

CREATE_STARTED_TASKS_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_cancelled = 0 AND is_task_started = 1 AND is_task_finished = 0
	ORDER BY start_timestamp DESC
;""".format(table = TABLE_NAME, view = STARTED_TASKS)

CREATE_FINISHED_TASKS_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_cancelled = 0 AND is_task_started = 1 AND is_task_finished = 1
	ORDER BY finish_timestamp DESC
;""".format(table = TABLE_NAME, view = FINISHED_TASKS)

CREATE_FAILED_TASKS_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_failed = 1
	ORDER BY creation_timestamp DESC
;""".format(table = TABLE_NAME, view = FAILED_TASKS)

CREATE_ACTIVE_TASK_COUNT_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT COUNT(*) AS task_count FROM {table}
;""".format(table = ACTIVE_TASKS, view = ACTIVE_TASK_COUNT)

CREATE_NEXT_ACTIVE_TASK_VIEW_SQL = """CREATE VIEW {view} AS
	SELECT * FROM {table}
	WHERE is_task_cancelled = 0 AND is_task_started = 0 AND is_task_finished = 0
	ORDER BY task_priority DESC, creation_timestamp ASC
	LIMIT 1
;""".format(table = TABLE_NAME, view = NEXT_ACTIVE_TASK)

CREATE_EXPERIMENT_REPORTS_VIEW_SQL = """CREATE VIEW experiment_reports AS
	SELECT
		creation_timestamp,
		experiment_folder,
		experiment_key,
		split_index,
		attempt_count,
		page_count,
		total_ad_count / attempt_count AS ads_per_attempt,
		total_ad_count,
		is_experiment_failed
	FROM (
		SELECT
			MIN(creation_timestamp) AS creation_timestamp,
			experiment_folder,
			experiment_key,
			split_index,
			COUNT(*) AS attempt_count,
			MAX(page_index) AS page_count,
			SUM(ad_count) AS total_ad_count,
			SUM(is_task_failed) AS is_experiment_failed
		FROM {table}
		GROUP BY experiment_folder, split_index
	)
	ORDER BY creation_timestamp DESC
;""".format(table = TABLE_NAME)

TABLE_EXISTS_SQL = """SELECT COUNT(*) = 1 FROM sqlite_master WHERE type = "table" AND name = "{table}";""".format(table = TABLE_NAME)

class QueueDB:
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

	def _has_tables(self):
		self.cursor.execute(TABLE_EXISTS_SQL)
		one_row = self.cursor.fetchone()
		table_exists = bool(one_row[0])
		return table_exists

	def _create_tables(self):
		if self.verbose:
			print("[QueueDB] Creating table '{}'...".format(TABLE_NAME))
		print(CREATE_TABLE_SQL)
		self.cursor.execute(CREATE_TABLE_SQL)

	def _create_indexes(self):
		if self.verbose:
			print("[QueueDB] Creating indexes...")
		print(CREATE_ACTIVE_TASKS_INDEX_SQL)
		self.cursor.execute(CREATE_ACTIVE_TASKS_INDEX_SQL)
		print(CREATE_FAILED_TASKS_INDEX_SQL)
		self.cursor.execute(CREATE_FAILED_TASKS_INDEX_SQL)
		print(CREATE_CANCELLED_TASKS_INDEX_SQL)
		self.cursor.execute(CREATE_CANCELLED_TASKS_INDEX_SQL)
		print(CREATE_QUEUED_TASKS_INDEX_SQL)
		self.cursor.execute(CREATE_QUEUED_TASKS_INDEX_SQL)
		print(CREATE_STARTED_TASKS_INDEX_SQL)
		self.cursor.execute(CREATE_STARTED_TASKS_INDEX_SQL)
		print(CREATE_FINISHED_TASKS_INDEX_SQL)
		self.cursor.execute(CREATE_FINISHED_TASKS_INDEX_SQL)

	def _create_views(self):
		if self.verbose:
			print("[QueueDB] Creating views...")
		print(CREATE_ACTIVE_TASKS_VIEW_SQL)
		self.cursor.execute(CREATE_ACTIVE_TASKS_VIEW_SQL)
		print(CREATE_QUEUED_TASKS_VIEW_SQL)
		self.cursor.execute(CREATE_QUEUED_TASKS_VIEW_SQL)
		print(CREATE_FAILED_TASKS_VIEW_SQL)
		self.cursor.execute(CREATE_FAILED_TASKS_VIEW_SQL)
		print(CREATE_CANCELLED_TASKS_VIEW_SQL)
		self.cursor.execute(CREATE_CANCELLED_TASKS_VIEW_SQL)
		print(CREATE_STARTED_TASKS_VIEW_SQL)
		self.cursor.execute(CREATE_STARTED_TASKS_VIEW_SQL)
		print(CREATE_FINISHED_TASKS_VIEW_SQL)
		self.cursor.execute(CREATE_FINISHED_TASKS_VIEW_SQL)
		print(CREATE_NEXT_ACTIVE_TASK_VIEW_SQL)
		self.cursor.execute(CREATE_NEXT_ACTIVE_TASK_VIEW_SQL)
		print(CREATE_ACTIVE_TASK_COUNT_VIEW_SQL)
		self.cursor.execute(CREATE_ACTIVE_TASK_COUNT_VIEW_SQL)
		print(CREATE_EXPERIMENT_REPORTS_VIEW_SQL)
		self.cursor.execute(CREATE_EXPERIMENT_REPORTS_VIEW_SQL)

	def _serialize_json(self, text):
		return json.dumps(text, indent = 2, sort_keys = True)

	def _deserialize_json(self, blob):
		return json.loads(blob)

	def open(self):
		if self.verbose:
			print()
			print("[QueueDB] Connecting to database...")
		self.connection = sqlite3.connect(self.db_path, detect_types = sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
		self.connection.row_factory = sqlite3.Row
		self.cursor = self.connection.cursor()

		if not self._has_tables():
			self._create_tables()
			self._create_indexes()
			self._create_views()

	def close(self):
		if self.verbose:
			print("[QueueDB] Committing changes to database...")
		self.connection.commit()

		if self.verbose:
			print("[QueueDB] Disconnecting from database...")
			print()
		self.connection.close()

	def create_task(self, experiment_spec, split_spec, page_spec, attempt_spec, continuation):
		if self.verbose:
			print("[QueueDB] Creating a new task...")

		all_specs = {**experiment_spec, **split_spec, **page_spec, **attempt_spec, **continuation}
		task_priority = all_specs["task_priority"]
		experiment_key = all_specs["experiment_key"]
		split_index = all_specs["split_index"]
		page_index = all_specs["page_index"]
		page_attempt = all_specs["page_attempt"]
		attempt_index = all_specs["attempt_index"]
		experiment_folder = all_specs["experiment_folder"]
		assert isinstance(task_priority, int)
		assert isinstance(experiment_key, str)
		assert isinstance(split_index, int)
		assert isinstance(page_index, int)
		assert isinstance(page_attempt, int)
		assert isinstance(attempt_index, int)
		assert isinstance(experiment_folder, str)

		experiment_spec_str = self._serialize_json(experiment_spec)
		split_spec_str = self._serialize_json(split_spec)
		page_spec_str = self._serialize_json(page_spec)
		attempt_spec_str = self._serialize_json(attempt_spec)
		continuation_str = self._serialize_json(continuation)
		is_task_failed = continuation["is_task_failed"] if "is_task_failed" in continuation else False

		sql = INSERT_CREATE_FAILED_TASK_SQL if is_task_failed else INSERT_CREATE_NORMAL_TASK_SQL
		self.cursor.execute(sql, (task_priority,
			experiment_key, split_index, page_index, page_attempt, attempt_index,
			experiment_spec_str, split_spec_str, page_spec_str, attempt_spec_str,
			continuation_str,
			experiment_folder,
		))

	def start_task(self, task_key):
		if self.verbose:
			print("[QueueDB] Starting task #{}...".format(task_key))
		assert isinstance(task_key, int)
		self.cursor.execute(UPDATE_START_TASK_SQL, (task_key, ))

	def finish_task(self, task_key):
		if self.verbose:
			print("[QueueDB] Finishing task #{}...".format(task_key))
		assert isinstance(task_key, int)
		self.cursor.execute(UPDATE_FINISH_TASK_SQL, (task_key, ))
	
	def cancel_task(self, task_key):
		if self.verbose:
			print("[QueueDB] Cancelling task #{}...".format(task_key))
		assert isinstance(task_key, int)
		self.cursor.execute(UPDATE_CANCEL_TASK_SQL, (task_key, ))

	def restart_task(self, task_key):
		if self.verbose:
			print("[QueueDB] Restarting task #{}...".format(task_key))
		assert isinstance(task_key, int)
		self.cursor.execute(UPDATE_RESTART_TASK_SQL, (task_key, ))
	
	def amend_task(self, task_key, finish_code, finish_log):
		if self.verbose:
			print("[QueueDB] Amending task #{} with logging information...".format(task_key))
		assert isinstance(task_key, int)
		assert isinstance(finish_code, int)

		finish_log_str = self._serialize_json(finish_log)
		access_token = finish_log["access_token"]
		ad_count = finish_log["ad_count"] if "ad_count" in finish_log else 0
		paging_cursor = finish_log["paging_cursor"] if "paging_cursor" in finish_log else None
		error_code = finish_log["error_code"] if "error_code" in finish_log else None

		assert isinstance(access_token, str)
		if ad_count is not None:
			assert isinstance(ad_count, int)
		if paging_cursor is not None:
			assert isinstance(paging_cursor, str)
		if error_code is not None:
			assert isinstance(error_code, int)

		self.cursor.execute(UPDATE_AMEND_TASK_SQL, (finish_code, finish_log_str, access_token, ad_count, paging_cursor, error_code, task_key, ))

	def get_active_task_count(self):
		if self.verbose:
			print("[QueueDB] Counting active tasks...")
		self.cursor.execute("SELECT * FROM {};".format(ACTIVE_TASK_COUNT))
		one_row = self.cursor.fetchone()
		task_count = one_row["task_count"]
		if self.verbose:
			print("    Counted {} active tasks".format(task_count))
		return task_count

	def get_next_active_task(self):
		if self.verbose:
			print("[QueueDB] Getting the next active task...")
		self.cursor.execute("SELECT task_key FROM {};".format(NEXT_ACTIVE_TASK))
		one_row = self.cursor.fetchone()
		task_key = one_row["task_key"]
		if self.verbose:
			print("    Next active task is task #{}".format(task_key))
		task = self.get_task(task_key)
		return task

	def get_task(self, task_key):
		if self.verbose:
			print("[QueueDB] Getting task #{}...".format(task_key))
		assert isinstance(task_key, int)
		
		self.cursor.execute("SELECT * FROM {} WHERE task_key = ?".format(TABLE_NAME), (task_key, ))
		one_row = self.cursor.fetchone()
		task = {
			"task_key": one_row["task_key"],
			"experiment_spec": self._deserialize_json(one_row["experiment_spec"]),
			"split_spec": self._deserialize_json(one_row["split_spec"]),
			"page_spec": self._deserialize_json(one_row["page_spec"]),
			"attempt_spec": self._deserialize_json(one_row["attempt_spec"]),
			"continuation": self._deserialize_json(one_row["continuation"]),
		}
		return task
		
	def get_task_as_dict(self, task_key):
		if self.verbose:
			print("[QueueDB] Getting task #{} as a dict...".format(task_key))
		assert isinstance(task_key, int)

		self.cursor.execute("SELECT * FROM {} WHERE task_key = ?".format(TABLE_NAME), (task_key, ))
		one_row = self.cursor.fetchone()
		task = dict(zip(one_row.keys(), one_row))
		return task

	def _print_task(self, task):
		print("    #{} :: {} :: {} :: exp={} / split={} / attempt={} / page={} ({})".format(
			task["task_key"],
			task["creation_timestamp"],
			task["task_priority"],
			task["experiment_key"],
			task["split_index"],
			task["attempt_index"],
			task["page_index"],
			task["page_attempt"],
		))
