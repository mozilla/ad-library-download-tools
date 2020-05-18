#!/usr/bin/env python3

from common import Constants

from datetime import datetime
import os
from selenium.webdriver import Firefox
import sqlalchemy
from sqlalchemy.orm import sessionmaker
   
from .google_ad_library_db import CreativeStats
from .google_ad_creatives_db import AdCreativeContent
from .google_ad_creatives_db import google_ad_creatives_create_tables

AD_LIBRARY_DB_FILENAME = "google_ad_library.sqlite"
AD_CREATEIVES_DB_FILENAME = "google_ad_creatives.sqlite"

class GoogleAdCreativeDownloadHelper:
	def __init__(self, verbose = True, echo = False, timestamp = None):
		self.verbose = verbose
		self.echo = echo
		self.timestamp = timestamp
		self._init_ad_library_db_session()
		self._init_ad_creatives_db_session()

	def _init_ad_library_db_session(self):
		db_path = os.path.join(Constants.DOWNLOADS_PATH, "google", self.timestamp, Constants.GOOGLE_AD_LIBRARY_DB_FILENAME)
		db_url = "sqlite:///{}".format(os.path.abspath(db_path))
		db_engine = sqlalchemy.create_engine(db_url, echo = self.echo)
		Session = sessionmaker(bind = db_engine)
		db_session = Session()
		self.ad_library = db_session

	def _init_ad_creatives_db_session(self):
		db_path = os.path.join(Constants.DOWNLOADS_PATH, "google", Constants.GOOGLE_AD_CREATIVES_DB_FILENAME)
		db_url = "sqlite:///{}".format(os.path.abspath(db_path))
		db_engine = sqlalchemy.create_engine(db_url, echo = self.echo)
		google_ad_creatives_create_tables(db_engine)
		Session = sessionmaker(bind = db_engine)
		db_session = Session()
		self.ad_creatives = db_session

	def download_ad_creative(self, ad_id, ad_url):
		print(ad_id, ad_url)

	def download_all_ad_creatives(self):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdCreative] ", timestamp)
			print("[AdCreative] Downloading all ad creatives...")
			print()

		query = self.ad_library.query(CreativeStats.ad_id, CreativeStats.ad_url)
		for i, row in enumerate(query):
			if self.verbose and i % 100 == 0:
				timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
				print("[AdCreative] ", timestamp)
				print("Number of ads = ", i, " / ", query.count())
				print()
				
			self.download_ad_creative(row.ad_id, row.ad_url)
		
			if i > 25:
				print()
				break

		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdCreative] Downloaded ad creative.")
			print("[AdCreative] ", timestamp)
			print()
