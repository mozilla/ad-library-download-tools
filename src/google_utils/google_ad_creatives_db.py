#!/usr/bin/env python3

from sqlalchemy import Table, Column, Integer, String, Text, DateTime, Boolean, MetaData

class GoogleAdCreativesDB:
	def __init__(self, engine):
		metadata = MetaData()
		self._define_tables(metadata)
		metadata.create_all(engine)

	def _define_tables(self, metadata):
		self.ad_content = Table("ad_content", metadata,
			Column("key", Integer, primary_key = True),
			Column("ad_id", String, unique = True, nullable = False),
			Column("ad_url", String, nullable = False),
			Column("ad_type", String, nullable = False),
			Column("ad_text", Text),
			Column("ad_html", Text),
			Column("screenshot_path", String),
			Column("is_skipped", Boolean, nullable = False),
			Column("is_url_accessed", Boolean, nullable = False),
			Column("is_ad_found", Boolean, nullable = False),
			Column("timestamp", DateTime, nullable = False),
		)
