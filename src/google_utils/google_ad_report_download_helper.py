#!/usr/bin/env python3

from common import Constants

import sqlalchemy
from sqlalchemy.orm import sessionmaker

import csv
from datetime import datetime
import os
import urllib.request
import zipfile

from .google_ad_library_tables import AdvertiserDeclaredStats, AdvertiserStats, AdvertiserWeeklySpend, CampaignTargeting, CreativeStats, GeoSpend, LastUpdated, TopKeywordsHistory
from .google_ad_library_tables import google_ad_library_create_tables

AD_REPORT_URL = "https://storage.googleapis.com/transparencyreport/google-political-ads-transparency-bundle.zip"
ZIP_FILENAME = "google-political-ads-transparency-bundle.zip"
CSV_FOLDER = "google-political-ads-transparency-bundle"
DB_FILENAME = "google_ad_library.sqlite"

class GoogleAdReportDownloadHelper:
	def __init__(self, verbose = True, echo = False, timestamp = None):
		self.verbose = verbose
		self.echo = echo
		self.timestamp = timestamp
		self.download_folder = None
		self._init_download_folder()
		self._init_db_session()
		
	def _init_download_folder(self):
		if self.download_folder is None:
			if self.timestamp is None:
				self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
			self.download_folder = os.path.join(Constants.DOWNLOADS_PATH, "google", self.timestamp)
		os.makedirs(self.download_folder, exist_ok = True)

	def _init_db_session(self):
		db_url = "sqlite:///{}".format(os.path.abspath(os.path.join(self.download_folder, DB_FILENAME)))
		db_engine = sqlalchemy.create_engine(db_url, echo = self.echo)
		google_ad_library_create_tables(db_engine)
		Session = sessionmaker(bind = db_engine)
		db_session = Session()
		self.db_session = db_session

	def _get_download_filename(self):
		return os.path.join(self.download_folder, ZIP_FILENAME)

	def _get_unzipped_folder(self):
		return os.path.join(self.download_folder, CSV_FOLDER)

	def _get_unzipped_csv_filename(self, table):
		return os.path.join(self.download_folder, CSV_FOLDER, "google-political-ads-{}.csv".format(table.replace("_", "-")))

	def download_political_ad_report(self):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdReport] {:s}".format(timestamp))
			print("[AdReport] Downloading ad transparency report...")

		filename = self._get_download_filename()
		urllib.request.urlretrieve(AD_REPORT_URL, filename = filename)

		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdReport] Downloaded ad transparency report: {:s}".format(filename))
			print("[AdReport] {:s}".format(timestamp))
			print()

	def unzip_political_ad_report(self):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdReport] {:s}".format(timestamp))
			print("[AdReport] Unzipping ad transparency report...")

		filename = self._get_download_filename()
		with zipfile.ZipFile(filename) as f:
			f.extractall(path = self.download_folder)

		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdReport] Unzipped ad transparency report: {:s}".format(self._get_unzipped_folder()))
			print("[AdReport] {:s}".format(timestamp))
			print()

	def _start_table_extraction(self, filename):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdReport] {:s}".format(timestamp))
			print("[AdReport] Extracting file: {:s}".format(filename))

	def _finish_table_extraction(self, filename):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[AdReport] {:s}".format(timestamp))
			print()
	
	def _get_header(self, row):
		header = {}
		for i, field in enumerate(row):
			header[field.lower()] = i
		return header
	
	def _get_date(self, s):
		if len(s) == 0:
			return None
		else:
			return datetime.strptime(s, "%Y-%m-%d").date()
	
	def _get_timestamp(self, s):
		if len(s) == 0:
			return None
		else:
			return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")
	
	def extract_advertiser_declared_stats_table(self):
		filename = self._get_unzipped_csv_filename("advertiser_declared_stats")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(AdvertiserDeclaredStats(
						advertiser_id = row[header["advertiser_id"]],
						advertiser_declared_name = row[header["advertiser_declared_name"]],
						advertiser_declared_regulatory_id = row[header["advertiser_declared_regulatory_id"]],
						advertiser_declared_scope = row[header["advertiser_declared_scope"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_advertiser_stats_table(self):
		filename = self._get_unzipped_csv_filename("advertiser_stats")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(AdvertiserStats(
						advertiser_id = row[header["advertiser_id"]],
						advertiser_name = row[header["advertiser_name"]],
						public_ids_list = row[header["public_ids_list"]],
						regions = row[header["regions"]],
						elections = row[header["elections"]],
						total_creatives = row[header["total_creatives"]],
						spend_usd = row[header["spend_usd"]],
						spend_eur = row[header["spend_eur"]],
						spend_inr = row[header["spend_inr"]],
						spend_bgn = row[header["spend_bgn"]],
						spend_hrk = row[header["spend_hrk"]],
						spend_czk = row[header["spend_czk"]],
						spend_dkk = row[header["spend_dkk"]],
						spend_huf = row[header["spend_huf"]],
						spend_pln = row[header["spend_pln"]],
						spend_ron = row[header["spend_ron"]],
						spend_sek = row[header["spend_sek"]],
						spend_gbp = row[header["spend_gbp"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_advertiser_weekly_spend_table(self):
		filename = self._get_unzipped_csv_filename("advertiser_weekly_spend")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(AdvertiserWeeklySpend(
						advertiser_id = row[header["advertiser_id"]],
						advertiser_name = row[header["advertiser_name"]],
						election_cycle = row[header["election_cycle"]],
						week_start_date = self._get_date(row[header["week_start_date"]]),
						spend_usd = row[header["spend_usd"]],
						spend_eur = row[header["spend_eur"]],
						spend_inr = row[header["spend_inr"]],
						spend_bgn = row[header["spend_bgn"]],
						spend_hrk = row[header["spend_hrk"]],
						spend_czk = row[header["spend_czk"]],
						spend_dkk = row[header["spend_dkk"]],
						spend_huf = row[header["spend_huf"]],
						spend_pln = row[header["spend_pln"]],
						spend_ron = row[header["spend_ron"]],
						spend_sek = row[header["spend_sek"]],
						spend_gbp = row[header["spend_gbp"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_campaign_targeting_table(self):
		filename = self._get_unzipped_csv_filename("campaign_targeting")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(CampaignTargeting(
						campaign_id = row[header["campaign_id"]],
						age_targeting = row[header["age_targeting"]],
						gender_targeting = row[header["gender_targeting"]],
						geo_targeting_included = row[header["geo_targeting_included"]],
						geo_targeting_excluded = row[header["geo_targeting_excluded"]],
						start_date = self._get_date(row[header["start_date"]]),
						end_date = self._get_date(row[header["end_date"]]),
						ads_list = row[header["ads_list"]],
						advertiser_id = row[header["advertiser_id"]],
						advertiser_name = row[header["advertiser_name"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_creative_stats_table(self):
		filename = self._get_unzipped_csv_filename("creative_stats")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(CreativeStats(
						ad_id = row[header["ad_id"]],
						ad_url = row[header["ad_url"]],
						ad_type = row[header["ad_type"]],
						regions = row[header["regions"]],
						advertiser_id = row[header["advertiser_id"]],
						advertiser_name = row[header["advertiser_name"]],
						ad_campaigns_list = row[header["ad_campaigns_list"]],
						date_range_start = self._get_date(row[header["date_range_start"]]),
						date_range_end = self._get_date(row[header["date_range_end"]]),
						num_of_days = row[header["num_of_days"]],
						impressions = row[header["impressions"]],
						spend_usd = row[header["spend_usd"]],
						age_targeting = row[header["age_targeting"]],
						gender_targeting = row[header["gender_targeting"]],
						geo_targeting_included = row[header["geo_targeting_included"]],
						geo_targeting_excluded = row[header["geo_targeting_excluded"]],
						first_served_timestamp = self._get_timestamp(row[header["first_served_timestamp"]]),
						last_served_timestamp = self._get_timestamp(row[header["last_served_timestamp"]]),
						spend_range_min_usd = row[header["spend_range_min_usd"]],
						spend_range_max_usd = row[header["spend_range_max_usd"]],
						spend_range_min_eur = row[header["spend_range_min_eur"]],
						spend_range_max_eur = row[header["spend_range_max_eur"]],
						spend_range_min_inr = row[header["spend_range_min_inr"]],
						spend_range_max_inr = row[header["spend_range_max_inr"]],
						spend_range_min_bgn = row[header["spend_range_min_bgn"]],
						spend_range_max_bgn = row[header["spend_range_max_bgn"]],
						spend_range_min_hrk = row[header["spend_range_min_hrk"]],
						spend_range_max_hrk = row[header["spend_range_max_hrk"]],
						spend_range_min_czk = row[header["spend_range_min_czk"]],
						spend_range_max_czk = row[header["spend_range_max_czk"]],
						spend_range_min_dkk = row[header["spend_range_min_dkk"]],
						spend_range_max_dkk = row[header["spend_range_max_dkk"]],
						spend_range_min_huf = row[header["spend_range_min_huf"]],
						spend_range_max_huf = row[header["spend_range_max_huf"]],
						spend_range_min_pln = row[header["spend_range_min_pln"]],
						spend_range_max_pln = row[header["spend_range_max_pln"]],
						spend_range_min_ron = row[header["spend_range_min_ron"]],
						spend_range_max_ron = row[header["spend_range_max_ron"]],
						spend_range_min_sek = row[header["spend_range_min_sek"]],
						spend_range_max_sek = row[header["spend_range_max_sek"]],
						spend_range_min_gbp = row[header["spend_range_min_gbp"]],
						spend_range_max_gbp = row[header["spend_range_max_gbp"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_geo_spend_table(self):
		filename = self._get_unzipped_csv_filename("geo_spend")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(GeoSpend(
						country = row[header["country"]],
						country_subdivision_primary = row[header["country_subdivision_primary"]],
						country_subdivision_secondary = row[header["country_subdivision_secondary"]],
						spend_usd = row[header["spend_usd"]],
						spend_eur = row[header["spend_eur"]],
						spend_inr = row[header["spend_inr"]],
						spend_bgn = row[header["spend_bgn"]],
						spend_hrk = row[header["spend_hrk"]],
						spend_czk = row[header["spend_czk"]],
						spend_dkk = row[header["spend_dkk"]],
						spend_huf = row[header["spend_huf"]],
						spend_pln = row[header["spend_pln"]],
						spend_ron = row[header["spend_ron"]],
						spend_sek = row[header["spend_sek"]],
						spend_gbp = row[header["spend_gbp"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_last_updated_table(self):
		pass

	def extract_top_keywords_history_table(self):
		filename = self._get_unzipped_csv_filename("top_keywords_history")
		self._start_table_extraction(filename)
		with open(filename) as f:
			reader = csv.reader(f)
			header = None
			for row in reader:
				if header is None:
					header = self._get_header(row)
				else:
					self.db_session.add(TopKeywordsHistory(
						election_cycle = row[header["election_cycle"]],
						report_date = self._get_date(row[header["report_date"]]),
						keyword_1 = row[header["keyword_1"]],
						spend_usd_1 = row[header["spend_usd_1"]],
						keyword_2 = row[header["keyword_2"]],
						spend_usd_2 = row[header["spend_usd_2"]],
						keyword_3 = row[header["keyword_3"]],
						spend_usd_3 = row[header["spend_usd_3"]],
						keyword_4 = row[header["keyword_4"]],
						spend_usd_4 = row[header["spend_usd_4"]],
						keyword_5 = row[header["keyword_5"]],
						spend_usd_5 = row[header["spend_usd_5"]],
						keyword_6 = row[header["keyword_6"]],
						spend_usd_6 = row[header["spend_usd_6"]],
						region = row[header["region"]],
						elections = row[header["elections"]]
					))
			self.db_session.commit()
		self._finish_table_extraction(filename)

	def extract_all_tables(self):
		self.extract_advertiser_stats_table()
		self.extract_advertiser_declared_stats_table()
		self.extract_advertiser_weekly_spend_table()
		self.extract_campaign_targeting_table()
		self.extract_creative_stats_table()
		self.extract_geo_spend_table()
		self.extract_last_updated_table()
		self.extract_top_keywords_history_table()
