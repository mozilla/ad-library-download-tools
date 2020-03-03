#!/usr/bin/env python3

from facebook_utils import QueueDB

class QueueManager:
	def __init__(self, db_folder = None, verbose = True):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self._db = QueueDB(db_folder = db_folder, verbose = False)

	def get_next_task(self):
		self._db.open()
		task_count = self._db.get_active_task_count()
		if task_count > 0:
			this_task = self._db.get_next_active_task()
		else:
			this_task = None
		self._db.close()
		return this_task

	def before_task(self):
		self._db.open()

	def start_task(self, task_key):
		self._db.start_task(task_key)

	def finish_task(self, task_key):
		self._db.finish_task(task_key)

	def amend_task(self, task_key, finish_code, finish_log):
		self._db.amend_task(task_key, finish_code, finish_log)

	def get_task_as_dict(self, task_key):
		return self._db.get_task_as_dict(task_key)

	def after_task(self):
		self._db.close()

	def create_next_task(self, experiment_spec, split_spec, page_spec, attempt_spec, continuation):
		self._db.open()
		self._db.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
		self._db.close()

	def create_next_tasks(self, experiment_spec, split_specs, page_spec, attempt_spec, continuation):
		self._db.open()
		for split_spec in split_specs:
			self._db.create_task(experiment_spec, split_spec, page_spec, attempt_spec, continuation)
		self._db.close()
