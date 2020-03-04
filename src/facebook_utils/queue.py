#!/usr/bin/env python3

from facebook_utils import QueueDB

class QueueManager:
	def __init__(self, db_folder = None, verbose = True):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self._db = QueueDB(db_folder = db_folder, verbose = False)

	def get_task(self, task_key):
		self._db.open()
		task = self._db.get_task(task_key)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Retrieved task #{}".format(task_key))
		return task
		
	def get_next_active_task(self):
		self._db.open()
		task_count = self._db.get_active_task_count()
		if task_count > 0:
			task = self._db.get_next_active_task()
		else:
			task = None
		self._db.close()
		if self.verbose:
			print("[QueueManager] Retrieved next task (#{})".format(task if "task_key" in task else None))
		return task

	def before_task(self):
		self._db.open()

	def run_start_task(self, task_key):
		self._db.start_task(task_key)

	def run_finish_task(self, task_key):
		self._db.finish_task(task_key)

	def run_amend_task(self, task_key):
		self._db.amend_task(task_key)

	def after_task(self):
		self._db.close()

	def amend_task(self, task_key, finish_code, finish_log):
		self._db.open()
		self._db.amend_task(task_key, finish_code, finish_log)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Amended task #{}".format(task_key))

	def start_task(self, task_key):
		self._db.open()
		self._db.start_task(task_key)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Started task #{}".format(task_key))

	def finish_task(self, task_key):
		self._db.open()
		self._db.finish_task(task_key)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Finished task #{}".format(task_key))

	def cancel_task(self, task_key):
		self._db.open()
		self._db.cancel_task(task_key)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Cancelled task #{}".format(task_key))

	def restart_task(self, task_key):
		self._db.open()
		self._db.restart_task(task_key)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Restarted task #{}".format(task_key))

	def get_task_as_dict(self, task_key):
		self._db.open()
		task = self._db.get_task_as_dict(task_key)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Retrieved task #{} as a dict".format(task_key))
		return task

	def create_task(self, experiment_spec, split_spec, page_spec, attempt_spec, continuation):
		self._db.open()
		self._db.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Create a new task.")

	def create_tasks(self, experiment_spec, split_specs, page_spec, attempt_spec, continuation):
		self._db.open()
		for split_spec in split_specs:
			self._db.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
		self._db.close()
		if self.verbose:
			print("[QueueManager] Create {} new tasks.".format(len(split_specs)))
