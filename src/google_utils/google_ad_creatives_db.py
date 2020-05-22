#!/usr/bin/env python3

from datetime import datetime
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
			Column("ad_text", Text, default = None),
			Column("ad_html", Text, default = None),
			Column("image_url", Text, default = None),
			Column("image_html", Text, default = None),
			Column("video_url", Text, default = None),
			Column("screenshot_path", String, default = None),
			Column("is_skipped", Boolean, default = False, nullable = False),
			Column("is_url_accessed", Boolean, default = False, nullable = False),
			Column("is_ad_found", Boolean, default = False, nullable = False),
			Column("is_image_downloaded", Boolean, default = False, nullable = False),
			Column("is_video_downloaded", Boolean, default = False, nullable = False),
			Column("timestamp", DateTime, default = datetime.now, nullable = False),
		)
