#!/usr/bin/env python3

from common import Constants

from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import sqlalchemy
from sqlalchemy.orm import sessionmaker

from .google_ad_library_db import AdvertiserDeclaredStats, AdvertiserStats, AdvertiserWeeklySpend, CampaignTargeting, CreativeStats, GeoSpend, LastUpdated, TopKeywordsHistory
from .google_ad_library_db import google_ad_library_create_tables

GC_SERVICE_ACCOUNT_KEY = "google_service_account_key.json"
GC_SCOPES = ["https://www.googleapis.com/auth/bigquery.readonly"]
GC_POLITICAL_ADS_PATH = "bigquery-public-data.google_political_ads"
GC_PAGE_SIZE = 10000

class GoogleBigQueryDownloadHelper:
	def __init__(self, verbose = True, echo = False, timestamp = None):
		self.verbose = verbose
		self.echo = echo
		self.timestamp = timestamp
		self.download_folder = None
		self._init_download_folder()
		self._init_db_session()
		self._init_gc_client()

	def _init_download_folder(self):
		if self.download_folder is None:
			if self.timestamp is None:
				self.timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
			self.download_folder = os.path.join(Constants.DOWNLOADS_PATH, "google", self.timestamp)
		os.makedirs(self.download_folder, exist_ok = True)

	def _init_db_session(self):
		db_url = "sqlite:///{}".format(os.path.abspath(os.path.join(self.download_folder, Constants.GOOGLE_AD_LIBRARY_DB_FILENAME)))
		db_engine = sqlalchemy.create_engine(db_url, echo = self.echo)
		google_ad_library_create_tables(db_engine)
		Session = sessionmaker(bind = db_engine)
		db_session = Session()
		self.db_session = db_session

	def _init_gc_client(self):
		gc_service_account_key_path = os.path.join(Constants.PREF_PATH, GC_SERVICE_ACCOUNT_KEY)
		gc_credientials = service_account.Credentials.from_service_account_file(gc_service_account_key_path, scopes = GC_SCOPES)
		gc_client = bigquery.Client(credentials = gc_credientials, project = gc_credientials.project_id)
		self.gc_client = gc_client

	def _get_table_path(self, table):
		return GC_POLITICAL_ADS_PATH + "." + table

	def _start_table_download(self, table):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[BigQuery] Start = {:s}".format(timestamp))
			print("[BigQuery] Table = {:s}".format(table))

	def _finish_stable_download(self, table):
		if self.verbose:
			timestamp = datetime.now().strftime("%-I:%M:%S %p @ %A, %B %-d, %Y")
			print("[BigQuery] Finish = {:s}".format(timestamp))

	def download_advertiser_declared_stats_table(self):
		table = self._get_table_path("advertiser_declared_stats")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(AdvertiserDeclaredStats(
					advertiser_id = row.advertiser_id,
					advertiser_declared_name = row.advertiser_declared_name,
					advertiser_declared_regulatory_id = row.advertiser_declared_regulartory_id,
					advertiser_declared_scope = row.advertiser_declared_scope
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_advertiser_stats_table(self):
		table = self._get_table_path("advertiser_stats")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(AdvertiserStats(
					advertiser_id = row.advertiser_id,
					advertiser_name = row.advertiser_name,
					public_ids_list = row.public_ids_list,
					regions = row.regions,
					elections = row.elections,
					total_creatives = row.total_creatives,
					spend_usd = row.spend_usd,
					spend_eur = row.spend_eur,
					spend_inr = row.spend_inr,
					spend_bgn = row.spend_bgn,
					spend_hrk = row.spend_hrk,
					spend_czk = row.spend_czk,
					spend_dkk = row.spend_dkk,
					spend_huf = row.spend_huf,
					spend_pln = row.spend_pln,
					spend_ron = row.spend_ron,
					spend_sek = row.spend_sek,
					spend_gbp = row.spend_gbp
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_advertiser_weekly_spend_table(self):
		table = self._get_table_path("advertiser_weekly_spend")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(AdvertiserWeeklySpend(
					advertiser_id = row.advertiser_id,
					advertiser_name = row.advertiser_name,
					election_cycle = row.election_cycle,
					week_start_date = row.week_start_date,
					spend_usd = row.spend_usd,
					spend_eur = row.spend_eur,
					spend_inr = row.spend_inr,
					spend_bgn = row.spend_bgn,
					spend_hrk = row.spend_hrk,
					spend_czk = row.spend_czk,
					spend_dkk = row.spend_dkk,
					spend_huf = row.spend_huf,
					spend_pln = row.spend_pln,
					spend_ron = row.spend_ron,
					spend_sek = row.spend_sek,
					spend_gbp = row.spend_gbp
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_campaign_targeting_table(self):
		table = self._get_table_path("campaign_targeting")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(CampaignTargeting(
					campaign_id = row.campaign_id,
					age_targeting = row.age_targeting,
					gender_targeting = row.gender_targeting,
					geo_targeting_included = row.geo_targeting_included,
					geo_targeting_excluded = row.geo_targeting_excluded,
					start_date = row.start_date,
					end_date = row.end_date,
					ads_list = row.ads_list,
					advertiser_id = row.advertiser_id,
					advertiser_name = row.advertiser_name
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_creative_stats_table(self):
		table = self._get_table_path("creative_stats")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(CreativeStats(
					ad_id = row.ad_id,
					ad_url = row.ad_url,
					ad_type = row.ad_type,
					regions = row.regions,
					advertiser_id = row.advertiser_id,
					advertiser_name = row.advertiser_name,
					ad_campaigns_list = row.ad_campaigns_list,
					date_range_start = row.date_range_start,
					date_range_end = row.date_range_end,
					num_of_days = row.num_of_days,
					impressions = row.impressions,
					spend_usd = row.spend_usd,
					age_targeting = row.age_targeting,
					gender_targeting = row.gender_targeting,
					geo_targeting_included = row.geo_targeting_included,
					geo_targeting_excluded = row.geo_targeting_excluded,
					first_served_timestamp = row.first_served_timestamp,
					last_served_timestamp = row.last_served_timestamp,
					spend_range_min_usd = row.spend_range_min_usd,
					spend_range_max_usd = row.spend_range_max_usd,
					spend_range_min_eur = row.spend_range_min_eur,
					spend_range_max_eur = row.spend_range_max_eur,
					spend_range_min_inr = row.spend_range_min_inr,
					spend_range_max_inr = row.spend_range_max_inr,
					spend_range_min_bgn = row.spend_range_min_bgn,
					spend_range_max_bgn = row.spend_range_max_bgn,
					spend_range_min_hrk = row.spend_range_min_hrk,
					spend_range_max_hrk = row.spend_range_max_hrk,
					spend_range_min_czk = row.spend_range_min_czk,
					spend_range_max_czk = row.spend_range_max_czk,
					spend_range_min_dkk = row.spend_range_min_dkk,
					spend_range_max_dkk = row.spend_range_max_dkk,
					spend_range_min_huf = row.spend_range_min_huf,
					spend_range_max_huf = row.spend_range_max_huf,
					spend_range_min_pln = row.spend_range_min_pln,
					spend_range_max_pln = row.spend_range_max_pln,
					spend_range_min_ron = row.spend_range_min_ron,
					spend_range_max_ron = row.spend_range_max_ron,
					spend_range_min_sek = row.spend_range_min_sek,
					spend_range_max_sek = row.spend_range_max_sek,
					spend_range_min_gbp = row.spend_range_min_gbp,
					spend_range_max_gbp = row.spend_range_max_gbp
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_geo_spend_table(self):
		table = self._get_table_path("geo_spend")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(GeoSpend(
					country = row.country,
					country_subdivision_primary = row.country_subdivision_primary,
					country_subdivision_secondary = row.country_subdivision_secondary,
					spend_usd = row.spend_usd,
					spend_eur = row.spend_eur,
					spend_inr = row.spend_inr,
					spend_bgn = row.spend_bgn,
					spend_hrk = row.spend_hrk,
					spend_czk = row.spend_czk,
					spend_dkk = row.spend_dkk,
					spend_huf = row.spend_huf,
					spend_pln = row.spend_pln,
					spend_ron = row.spend_ron,
					spend_sek = row.spend_sek,
					spend_gbp = row.spend_gbp
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_last_updated_table(self):
		table = self._get_table_path("last_updated")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(LastUpdated(
					report_data_updated_date = row.report_data_updated_date
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_top_keywords_history_table(self):
		table = self._get_table_path("top_keywords_history")
		self._start_table_download(table)
		rows = self.gc_client.list_rows(table, page_size = GC_PAGE_SIZE)
		for page in rows.pages:
			if self.verbose:
				print("[BigQuery] Page #{} ({} rows)".format(rows.page_number, rows.num_results))
			for row in page:
				self.db_session.add(TopKeywordsHistory(
					election_cycle = row.election_cycle,
					report_date = row.report_date,
					keyword_1 = row.keyword_1,
					spend_usd_1 = row.spend_usd_1,
					keyword_2 = row.keyword_2,
					spend_usd_2 = row.spend_usd_2,
					keyword_3 = row.keyword_3,
					spend_usd_3 = row.spend_usd_3,
					keyword_4 = row.keyword_4,
					spend_usd_4 = row.spend_usd_4,
					keyword_5 = row.keyword_5,
					spend_usd_5 = row.spend_usd_5,
					keyword_6 = row.keyword_6,
					spend_usd_6 = row.spend_usd_6,
					region = row.region,
					elections = row.elections
				))
			self.db_session.commit()
		self._finish_stable_download(table)

	def download_all_tables(self):
		self.download_advertiser_stats_table()
		self.download_advertiser_declared_stats_table()
		self.download_advertiser_weekly_spend_table()
		self.download_campaign_targeting_table()
		self.download_creative_stats_table()
		self.download_geo_spend_table()
		self.download_last_updated_table()
		self.download_top_keywords_history_table()
