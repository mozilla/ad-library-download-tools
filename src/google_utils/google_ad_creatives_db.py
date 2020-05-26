#!/usr/bin/env python3

from datetime import datetime
from sqlalchemy import Table, Column, Integer, String, Text, DateTime, Boolean, MetaData

class GoogleAdCreativesDB:
	def __init__(self, engine):
		metadata = MetaData()
		self._define_tables(metadata)
		metadata.create_all(engine)

	def _define_tables(self, metadata):
		self.text_ads = Table("text_ads", metadata,
			Column("key", Integer, primary_key = True),
			Column("ad_id", String, unique = True, nullable = False),
			Column("ad_url", String, nullable = False),
			Column("ad_html", Text, default = None),
			Column("ad_width", Integer, default = None),
			Column("ad_height", Integer, default = None),
			Column("ad_text", Text, default = None),
			Column("is_url_accessed", Boolean, default = False, nullable = False),
			Column("is_ad_found", Boolean, default = False, nullable = False),
			Column("is_ad_removed", Boolean, default = False, nullable = False),
			Column("is_known_error", Boolean, default = False, nullable = False),
			Column("is_unknown_error", Boolean, default = False, nullable = False),
			Column("screenshot_path", String, default = None),
			Column("timestamp", DateTime, default = datetime.now, nullable = False),
		)
		
		self.image_ads = Table("image_ads", metadata,
			Column("key", Integer, primary_key = True),
			Column("ad_id", String, unique = True, nullable = False),
			Column("ad_url", String, nullable = False),
			Column("ad_html", Text, default = None),
			Column("ad_width", Integer, default = None),
			Column("ad_height", Integer, default = None),
			Column("image_url", String, default = None),
			Column("in_iframe", Boolean, default = False, nullable = False),
			Column("in_img", Boolean, default = False, nullable = False),
			Column("is_url_accessed", Boolean, default = False, nullable = False),
			Column("is_ad_found", Boolean, default = False, nullable = False),
			Column("is_ad_removed", Boolean, default = False, nullable = False),
			Column("is_known_error", Boolean, default = False, nullable = False),
			Column("is_unknown_error", Boolean, default = False, nullable = False),
			Column("screenshot_path", String, default = None),
			Column("timestamp", DateTime, default = datetime.now, nullable = False),
		)

		self.video_ads = Table("video_ads", metadata,
			Column("key", Integer, primary_key = True),
			Column("ad_id", String, unique = True, nullable = False),
			Column("ad_url", String, nullable = False),
			Column("ad_html", Text, default = None),
			Column("ad_width", Integer, default = None),
			Column("ad_height", Integer, default = None),
			Column("youtube_url", String, default = None),
			Column("youtube_id", String, default = None),
			Column("video_url", String, default = None),
			Column("in_iframe", Boolean, default = False, nullable = False),
			Column("in_video", Boolean, default = False, nullable = False),
			Column("is_url_accessed", Boolean, default = False, nullable = False),
			Column("is_ad_found", Boolean, default = False, nullable = False),
			Column("is_ad_removed", Boolean, default = False, nullable = False),
			Column("is_known_error", Boolean, default = False, nullable = False),
			Column("is_unknown_error", Boolean, default = False, nullable = False),
			Column("screenshot_path", String, default = None),
			Column("timestamp", DateTime, default = datetime.now, nullable = False),
		)
