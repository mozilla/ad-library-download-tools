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

TEXT_AD_TYPE = "Text"
IMAGE_AD_TYPE = "Image"
VIDEO_AD_TYPE = "Video"

TIMEOUT_MAX_SECS = 5
CREATIVE_WRAPPER_CLASS_NAME = "creative-wrapper"

TEXT_AD_TAG_NAME = "text-ad"

IMAGE_AD_IFRAME_TAG_NAME = "iframe"
IMAGE_AD_CONTAINER_ID = "google_image_div"
IMAGE_AD_IMAGE_CLASS_NAME = "img_ad"
VIDEO_AD_TAG_NAME = "video-ad"

AdInfo = namedtuple("AdInfo", ["id", "url", "type"])

class GoogleAdCreativesDownloadHelper:
	def __init__(self, timestamp, headless = False, shuffle = False, echo = False, verbose = True):
		self.verbose = verbose
		self.echo = echo
		self.timestamp = timestamp
		self.headless = headless
		self.shuffle = shuffle
		self.driver = None
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
		if self.driver is None:
			print("[AdCreatives] {:s}".format(self._timestamp()))
			print("[AdCreatives] Starting WebDriver...")
			opts = webdriver.FirefoxOptions()
			opts.headless = self.headless
			self.driver = webdriver.Firefox(options = opts)
			print("[AdCreatives] Started WebDriver")
			self.driver.set_window_size(Constants.GOOGLE_WINDOW_WIDTH, Constants.GOOGLE_WINDOW_HEIGHT)
			print("[AdCreatives] Set driver to {:d}px by {:d}px".format(Constants.GOOGLE_WINDOW_WIDTH, Constants.GOOGLE_WINDOW_HEIGHT))
			print()
	
	def _timestamp(self):
		return datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")

	def _stop_webdriver(self):
		if self.driver is not None:
			print("[AdCreatives] {:s}".format(self._timestamp()))
			print("[AdCreatives] Stopping WebDriver...")
			self.driver.close()
			self.driver = None
			print("[AdCreatives] Stopped WebDriver")
			print()

	def _get_remaining_ad_ids(self, ad_type = None):
		if self.verbose:
			print("[AdCreatives] {:s}".format(self._timestamp()))
			print("[AdCreatives] Counting ads...")

		if ad_type is None:
			s = select([self.library_db.creative_stats_table.c.ad_id]).distinct()
		else:
			s = select([self.library_db.creative_stats_table.c.ad_id]).where(self.library_db.creative_stats_table.c.ad_type == ad_type).distinct()
		all_ad_ids = [row[0] for row in self.library_conn.execute(s)]
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

	def _get_ad_info(self, ad_id):
		s = select([self.library_db.creative_stats_table]).where(self.library_db.creative_stats_table.c.ad_id == ad_id)
		row = self.library_conn.execute(s).fetchone()
		ad_info = AdInfo(
			id = ad_id,
			url = row[self.library_db.creative_stats_table.c.ad_url],
			type = row[self.library_db.creative_stats_table.c.ad_type],
		)
		return ad_info

	def _download_text_ad(self, ad_info, screenshot_success = False, screenshot_error = True):
		ad_text = None
		ad_html = None
		screenshot_path = None
		is_url_accessed = False
		is_ad_found = False

		self.driver.get(ad_info.url)
		is_url_accessed = True
		print("Opened webpage.")
		try:
			elem = WebDriverWait(self.driver, TIMEOUT_MAX_SECS).until(expected_conditions.visibility_of_element_located((By.CLASS_NAME, CREATIVE_WRAPPER_CLASS_NAME)))
			print("Waited and located creative wrapper.")
			try:
				ad_elem = elem.find_element_by_tag_name(TEXT_AD_TAG_NAME)
				print("Located text ad element.")
				if screenshot_success:
					screenshot_path = os.path.join(self.screenshot_text_ads_folder, "{:s}.png".format(ad_info.id))
					ad_elem.screenshot(os.path.abspath(screenshot_path))
					print("Took a screenshot.")
				ad_text = ad_elem.text
				ad_html = ad_elem.get_attribute("outerHTML")
				is_ad_found = True
				print("Extracted text and html content of the text ad.")

			except NoSuchElementException:
				if screenshot_error:
					screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
					self.driver.get_screenshot_as_file(os.path.abspath(screenshot_path))
				print("Cannot locate text ad element.")

		except TimeoutException:
			if screenshot_error:
				screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
				self.driver.get_screenshot_as_file(os.path.abspath(screenshot_path))
			print("Cannot locate creative wrapper after waiting {:d} seconds".format(TIMEOUT_MAX_SECS))

		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"ad_text": ad_text,
			"ad_html": ad_html,
			"screenshot_path": screenshot_path,
			"is_url_accessed": is_url_accessed,
			"is_ad_found": is_ad_found,
		})
		print()

	def _download_image_ad(self, ad_info, screenshot_success = False, screenshot_error = True):
		image_url = None
		screenshot_path = None
		is_skipped = False
		is_url_accessed = False
		is_ad_found = False

		self.driver.get(ad_info.url)
		is_url_accessed = True
		print("Opened webpage.")
		try:
			elem = WebDriverWait(self.driver, TIMEOUT_MAX_SECS).until(expected_conditions.visibility_of_element_located((By.CLASS_NAME, CREATIVE_WRAPPER_CLASS_NAME)))
			print("Waited and located creative wrapper.")
			try:
				WebDriverWait(self.driver, TIMEOUT_MAX_SECS).until(expected_conditions.frame_to_be_available_and_switch_to_it((By.TAG_NAME, IMAGE_AD_IFRAME_TAG_NAME)))
				print("Switched to iframe containing image ad.")
				try:
					ad_elem = WebDriverWait(self.driver, TIMEOUT_MAX_SECS).until(expected_conditions.visibility_of_element_located((By.ID, IMAGE_AD_CONTAINER_ID)))
					print("Waited and located image ad element.")
					if screenshot_success:
						screenshot_path = os.path.join(self.screenshot_image_ads_folder, "{:s}.png".format(ad_info.id))
						ad_elem.screenshot(os.path.abspath(screenshot_path))
						print("Took a screenshot.")
					image_url = ad_elem.find_element_by_class_name(IMAGE_AD_IMAGE_CLASS_NAME).get_attribute("src")
					image_html = ad_elem.get_attribute("outerHTML")
					is_ad_found = True
					print("Extracted image source URL of the image ad.")
					
				except TimeoutException:
					if screenshot_error:
						screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
						self.driver.get_screenshot_as_file(os.path.abspath(screenshot_path))
					print("Cannot locate image ad element.")

			except TimeoutException:
				if screenshot_error:
					screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
					self.driver.get_screenshot_as_file(os.path.abspath(screenshot_path))
				print("Cannot locate iframe containing image ad.")

		except TimeoutException:
			if screenshot_error:
				screenshot_path = os.path.join(self.screenshot_errors_folder, "{:s}.png".format(ad_info.id))
				self.driver.get_screenshot_as_file(os.path.abspath(screenshot_path))
			print("Cannot locate creative wrapper after waiting {:d} seconds".format(TIMEOUT_MAX_SECS))

		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"image_url": image_url,
			"image_html": image_html,
			"screenshot_path": screenshot_path,
			"is_url_accessed": is_url_accessed,
			"is_ad_found": is_ad_found,
		})
		print()

	def _download_vidoe_ad(self, ad_info, screenshot_success = False, screenshot_error = True):
		print("Skipping video ads.")
		self.conn.execute(self.db.ad_content.insert(), {
			"ad_id": ad_info.id,
			"ad_url": ad_info.url,
			"ad_type": ad_info.type,
			"video_url": None,
			"screenshot_path": None,
			"is_skipped": True,
			"is_url_accessed": False,
			"is_ad_found": False,
		})
		print()

	def _download_unknown_ad(self, ad_info, screenshot_success = False, screenshot_error = True):
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
		})
		print()

	def _download_ad_creative(self, index, total_count, ad_id, screenshot_success = False, screenshot_error = True):
		ad_info = self._get_ad_info(ad_id)

		print("[AdCreatives] {:s}".format(self._timestamp()))
		print("[AdCreatives] Downloading remaining ad {:,d} of {:,d}: {:s}...".format(index, total_count, ad_info.url))
		if ad_info.type == TEXT_AD_TYPE:
			self._download_text_ad(ad_info, screenshot_success = screenshot_success, screenshot_error = screenshot_error)
		elif ad_info.type == IMAGE_AD_TYPE:
			self._download_image_ad(ad_info, screenshot_success = screenshot_success, screenshot_error = screenshot_error)
		elif ad_info.type == VIDEO_AD_TYPE:
			self._download_vidoe_ad(ad_info, screenshot_success = screenshot_success, screenshot_error = screenshot_error)
		else:
			self._download_unknown_ad(ad_info, screenshot_success = screenshot_success, screenshot_error = screenshot_error)

	def download_ad_creatives(self, ad_type, limit, screenshot):
		if self.verbose:
			print("[AdCreatives] {:s}".format(self._timestamp()))
			print("[AdCreatives] Downloading {:,d} ad creatives (type = {:s})...".format(limit, ad_type))
			print()

		self._start_webdriver()
		remaining_ad_ids = self._get_remaining_ad_ids(ad_type = ad_type)
		if self.shuffle:
			random.shuffle(remaining_ad_ids)
		for i, ad_id in enumerate(remaining_ad_ids):
			if self.verbose and i % 100 == 0:
				print("[AdCreatives] {:s}".format(self._timestamp()))
				print("Number of remaining ads (type = {:s}) = {:,d} / {:,d}".format(ad_type, i + 1, len(remaining_ad_ids)))
				print()
			self._download_ad_creative(i + 1, len(remaining_ad_ids), ad_id, screenshot_success = screenshot)
			if i + 1 >= limit:
				print()
				break
			if i + 1 % 1000 == 0:
				self._stop_webdriver()
				self._start_webdriver()
		self._stop_webdriver()

		if self.verbose:
			print("[AdCreatives] Downloaded {:,d} ad creatives (type = {:s})".format(limit, ad_type))
			print("[AdCreatives] {:s}".format(self._timestamp()))
			print()

	def download_text_ads(self, limit, screenshot):
		self.download_ad_creatives(TEXT_AD_TYPE, limit, screenshot)
		
	def download_image_ads(self, limit, screenshot):
		self.download_ad_creatives(IMAGE_AD_TYPE, limit, screenshot)
		
	def download_video_ads(self, limit, screenshot):
		self.download_ad_creatives(VIDEO_AD_TYPE, limit, screenshot)
