#!/usr/bin/env python3

from common import Constants
from google_utils import GoogleAdLibraryDB, GoogleAdCreativesDB

from collections import namedtuple
from datetime import datetime
import os
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import sqlalchemy
from sqlalchemy.sql import select, func

TIMEOUT_MAX_SECS = 10
CREATIVE_WRAPPER_CLASS_NAME = "creative-wrapper"
TEXT_AD_TAG_NAME = "text-ad"
IMAGE_AD_TAG_NAME = "image-ad"
VIDEO_AD_TAG_NAME = "video-ad"

AdInfo = namedtuple("AdInfo", ["id", "url", "type"])

class GoogleAdCreativesDownloadHelper:
	def __init__(self, verbose = True, echo = False, timestamp = None, headless = False, shuffle = False):
		self.verbose = verbose
		self.echo = echo
		self.timestamp = timestamp
		self.headless = headless
		self.shuffle = shuffle
		self.download_folder = None
		self.screenshot_text_ads_folder = None
		self.screenshot_image_ads_folder = None
		self.screenshot_video_ads_folder = None
		self.screenshot_errors_folder = None
		self._init_download_folder()
		self._init_ad_library_db_session()
		self._init_ad_creatives_db_session()

	def _init_download_folder(self):
		if self.download_folder is None:
			if self.timestamp is None:
				self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
			self.download_folder = os.path.join(Constants.DOWNLOADS_PATH, Constants.GOOGLE_DOWNLOADS_FOLDER, self.timestamp)
			self.screenshot_text_ads_folder = os.path.join(self.download_folder, Constants.GOOLGE_TEXT_AD_SCREENSHOTS)
			self.screenshot_image_ads_folder = os.path.join(self.download_folder, Constants.GOOLGE_IMAGE_AD_SCREENSHOTS)
			self.screenshot_video_ads_folder = os.path.join(self.download_folder, Constants.GOOLGE_VIDEO_AD_SCREENSHOTS)
			self.screenshot_errors_folder = os.path.join(self.download_folder, Constants.GOOLGE_ERROR_SCREENSHOTS)
		os.makedirs(self.screenshot_text_ads_folder, exist_ok = True)
		os.makedirs(self.screenshot_image_ads_folder, exist_ok = True)
		os.makedirs(self.screenshot_video_ads_folder, exist_ok = True)
		os.makedirs(self.screenshot_errors_folder, exist_ok = True)

	def _init_ad_library_db_session(self):
		path = os.path.join(Constants.DOWNLOADS_PATH, Constants.GOOGLE_DOWNLOADS_FOLDER, self.timestamp, Constants.GOOGLE_AD_LIBRARY_DB_FILENAME)
		url = "sqlite:///{}".format(os.path.abspath(path))
		engine = sqlalchemy.create_engine(url, echo = self.echo)
		self.library_conn = engine.connect()
		self.library_db = GoogleAdLibraryDB(engine)

	def _init_ad_creatives_db_session(self):
		path = os.path.join(Constants.DOWNLOADS_PATH, Constants.GOOGLE_DOWNLOADS_FOLDER, Constants.GOOGLE_AD_CREATIVES_DB_FILENAME)
		url = "sqlite:///{}".format(os.path.abspath(path))
		engine = sqlalchemy.create_engine(url, echo = self.echo)
		self.conn = engine.connect()
		self.db = GoogleAdCreativesDB(engine)

	def _start_webdriver(self):
		timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		print("[AdCreatives] {:s}".format(timestamp))
		print("[AdCreatives] Starting WebDriver...")
		opts = webdriver.FirefoxOptions()
		opts.headless = self.headless
		driver = webdriver.Firefox(options = opts)
		print()
		return driver

	def _stop_webdriver(self, driver):
		timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		print("[AdCreatives] {:s}".format(timestamp))
		print("[AdCreatives] Stopping WebDriver...")
		driver.close()
		print()

	def get_remaining_ad_ids(self):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdCreatives] {:s}".format(timestamp))
			print("[AdCreatives] Counting ads...")

		all_ad_ids = [row[0] for row in self.library_conn.execute(select([self.library_db.creative_stats_table.c.ad_id]).distinct())]
		all_set = frozenset(all_ad_ids)
		downloaded_ad_ids = [row[0] for row in self.conn.execute(select([self.db.ad_content.c.ad_id]).distinct())]
		downloaded_set = frozenset(downloaded_ad_ids)
		remaining_set = all_set.difference(downloaded_set)
		remaining_ad_ids = [ad_id for ad_id in all_ad_ids if ad_id in remaining_set]

		if self.verbose:
			print("[AdCreatives]      Total ads = {:9,d}".format(len(all_ad_ids)))
			print("[AdCreatives] Downloaded ads = {:9,d}".format(len(downloaded_ad_ids)))
			print("[AdCreatives]  Remaining ads = {:9,d}".format(len(remaining_ad_ids)))
			print()

		return remaining_ad_ids

	def get_ad_info(self, ad_id):
		s = select([self.library_db.creative_stats_table]).where(self.library_db.creative_stats_table.c.ad_id == ad_id)
		row = self.library_conn.execute(s).fetchone()
		ad_info = AdInfo(
			id = ad_id,
			url = row[self.library_db.creative_stats_table.c.ad_url],
			type = row[self.library_db.creative_stats_table.c.ad_type],
		)
		return ad_info

	def download_text_ad(self, driver, ad_info):
		ad_text = None
		ad_html = None
		screenshot_path = None
		is_skipped = False
		is_url_accessed = False
		is_ad_found = False

		driver.get(ad_info.url)
		try:
			elem = WebDriverWait(driver, TIMEOUT_MAX_SECS).until(expected_conditions.presence_of_element_located((By.CLASS_NAME, CREATIVE_WRAPPER_CLASS_NAME)))
			is_url_accessed = True

			try:
				ad_elem = elem.find_element_by_tag_name(TEXT_AD_TAG_NAME)
				screenshot_path = os.path.join(self.screenshot_text_ads_folder, "{:s}.png".format(ad_info.id))
				elem.screenshot(os.path.abspath(screenshot_path))
				ad_text = ad_elem.text
				ad_html = ad_elem.get_attribute("outerHTML")
				is_ad_found = True
				print("Extracted ad text and html content.")

			except NoSuchElementException:
				screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
				elem.screenshot(os.path.abspath(screenshot_path))
				print("Cannot locate the text ad in '{:s}' element.".format(TEXT_AD_TAG_NAME))

		except TimeoutException:
			screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
			driver.get_screenshot_as_file(os.path.abspath(screenshot_path))
			print("Cannot download ad after waiting {:d} seconds".format(TIMEOUT_MAX_SECS))

		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"ad_text": ad_text,
			"ad_html": ad_html,
			"screenshot_path": screenshot_path,
			"is_skipped": False,
			"is_url_accessed": is_url_accessed,
			"is_ad_found": is_ad_found,
			"timestamp": datetime.now(),
		})
		print()

	def download_image_ad(self, driver, ad_info):
		print("Skipping image ads.")
		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"ad_text": None,
			"ad_html": None,
			"screenshot_path": None,
			"is_skipped": True,
			"is_url_accessed": False,
			"is_ad_found": False,
			"timestamp": datetime.now(),
		})
		print()

	def download_vidoe_ad(self, driver, ad_info):
		print("Skipping video ads.")
		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"ad_text": None,
			"ad_html": None,
			"screenshot_path": None,
			"is_skipped": True,
			"is_url_accessed": False,
			"is_ad_found": False,
			"timestamp": datetime.now(),
		})
		print()

	def download_unknown_ad(self, driver, ad_info):
		print("Encountered unknown ad type: {:s}".format(ad_info.type))
		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"ad_text": None,
			"ad_html": None,
			"screenshot_path": None,
			"is_skipped": True,
			"is_url_accessed": False,
			"is_ad_found": False,
			"timestamp": datetime.now(),
		})
		print()

	def download_ad_creative(self, driver, index, ad_id):
		ad_info = self.get_ad_info(ad_id)

		timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
		print("[AdCreatives] {:s}".format(timestamp))
		print("[AdCreatives] Loading ad #{:d}: {:s} ({:s})".format(index + 1, ad_info.url, ad_info.type))
		if ad_info.type == "Text":
			self.download_text_ad(driver, ad_info)
		elif ad_info.type == "Image":
			self.download_image_ad(driver, ad_info)
		elif ad_info.type == "Video":
			self.download_vidoe_ad(driver, ad_info)
		else:
			self.download_unknown_ad(driver, ad_info)

	def download_all_ad_creatives(self):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdCreatives] {:s}".format(timestamp))
			print("[AdCreatives] Downloading all ad creatives...")
			print()

		driver = self._start_webdriver()
		driver.set_window_size(Constants.GOOGLE_WINDOW_WIDTH, Constants.GOOGLE_WINDOW_HEIGHT)
		remaining_ad_ids = self.get_remaining_ad_ids()
		if self.shuffle:
			random.shuffle(remaining_ad_ids)
		for i, ad_id in enumerate(remaining_ad_ids):
			if self.verbose and i % 1000 == 0:
				timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
				print("[AdCreatives] {:s}".format(timestamp))
				print("Number of ads = {:,d} / {:,d}".format(i, len(remaining_ad_ids)))
				print()
				self._stop_webdriver(driver)
				driver = self._start_webdriver()

			self.download_ad_creative(driver, i, ad_id)
			if i+1 >= 15000 * 4:
				print()
				break
		self._stop_webdriver(driver)

		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdCreatives] Downloaded ad creative.")
			print("[AdCreatives] {:s}".format(timestamp))
			print()
