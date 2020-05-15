#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AdvertiserDeclaredStats(Base):
	__tablename__ = "advertiser_declared_stats"
	key = Column(Integer, primary_key = True)
	advertiser_id = Column(String)
	advertiser_declared_name = Column(String)
	advertiser_declared_regulatory_id = Column(String)
	advertiser_declared_scope = Column(String)

class AdvertiserStats(Base):
	__tablename__ = "advertiser_stats"
	key = Column(Integer, primary_key = True)
	advertiser_id = Column(String)
	advertiser_name = Column(String)
	public_ids_list = Column(String)
	regions = Column(String)
	elections = Column(String)
	total_creatives = Column(Integer)
	spend_usd = Column(Integer)
	spend_eur = Column(Integer)
	spend_inr = Column(Integer)
	spend_bgn = Column(Integer)
	spend_hrk = Column(Integer)
	spend_czk = Column(Integer)
	spend_dkk = Column(Integer)
	spend_huf = Column(Integer)
	spend_pln = Column(Integer)
	spend_ron = Column(Integer)
	spend_sek = Column(Integer)
	spend_gbp = Column(Integer)

class AdvertiserWeeklySpend(Base):
	__tablename__ = "advertiser_weekly_spend"
	key = Column(Integer, primary_key = True)
	advertiser_id = Column(String)
	advertiser_name = Column(String)
	election_cycle = Column(String)
	week_start_date = Column(Date)
	spend_usd = Column(Integer)
	spend_eur = Column(Integer)
	spend_inr = Column(Integer)
	spend_bgn = Column(Integer)
	spend_hrk = Column(Integer)
	spend_czk = Column(Integer)
	spend_dkk = Column(Integer)
	spend_huf = Column(Integer)
	spend_pln = Column(Integer)
	spend_ron = Column(Integer)
	spend_sek = Column(Integer)
	spend_gbp = Column(Integer)

class CampaignTargeting(Base):
	__tablename__ = "campaign_targeting"
	key = Column(Integer, primary_key = True)
	campaign_id = Column(String)
	age_targeting = Column(String)
	gender_targeting = Column(String)
	geo_targeting_included = Column(String)
	geo_targeting_excluded = Column(String)
	start_date = Column(Date)
	end_date = Column(Date)
	ads_list = Column(String)
	advertiser_id = Column(String)
	advertiser_name = Column(String)

class CreativeStats(Base):
	__tablename__ = "creative_stats"
	key = Column(Integer, primary_key = True)
	ad_id = Column(String)
	ad_url = Column(String)
	ad_type = Column(String)
	regions = Column(String)
	advertiser_id = Column(String)
	advertiser_name = Column(String)
	ad_campaigns_list = Column(String)
	date_range_start = Column(Date)
	date_range_end = Column(Date)
	num_of_days = Column(Integer)
	impressions = Column(String)
	spend_usd = Column(String)
	age_targeting = Column(String)
	gender_targeting = Column(String)
	geo_targeting_included = Column(String)
	geo_targeting_excluded = Column(String)
	first_served_timestamp = Column(Date)
	last_served_timestamp = Column(Date)
	spend_range_min_usd = Column(Integer)
	spend_range_max_usd = Column(Integer)
	spend_range_min_eur = Column(Integer)
	spend_range_max_eur = Column(Integer)
	spend_range_min_inr = Column(Integer)
	spend_range_max_inr = Column(Integer)
	spend_range_min_bgn = Column(Integer)
	spend_range_max_bgn = Column(Integer)
	spend_range_min_hrk = Column(Integer)
	spend_range_max_hrk = Column(Integer)
	spend_range_min_czk = Column(Integer)
	spend_range_max_czk = Column(Integer)
	spend_range_min_dkk = Column(Integer)
	spend_range_max_dkk = Column(Integer)
	spend_range_min_huf = Column(Integer)
	spend_range_max_huf = Column(Integer)
	spend_range_min_pln = Column(Integer)
	spend_range_max_pln = Column(Integer)
	spend_range_min_ron = Column(Integer)
	spend_range_max_ron = Column(Integer)
	spend_range_min_sek = Column(Integer)
	spend_range_max_sek = Column(Integer)
	spend_range_min_gbp = Column(Integer)
	spend_range_max_gbp = Column(Integer)

class GeoSpend(Base):
	__tablename__ = "geo_spend"
	key = Column(Integer, primary_key = True)
	country = Column(String)
	country_subdivision_primary = Column(String)
	country_subdivision_secondary = Column(String)
	spend_usd = Column(Integer)
	spend_eur = Column(Integer)
	spend_inr = Column(Integer)
	spend_bgn = Column(Integer)
	spend_hrk = Column(Integer)
	spend_czk = Column(Integer)
	spend_dkk = Column(Integer)
	spend_huf = Column(Integer)
	spend_pln = Column(Integer)
	spend_ron = Column(Integer)
	spend_sek = Column(Integer)
	spend_gbp = Column(Integer)

class LastUpdated(Base):
	__tablename__ = "last_updated"
	key = Column(Integer, primary_key = True)
	report_data_updated_date = Column(Date)

class TopKeywordsHistory(Base):
	__tablename__ = "top_keywords_history"
	key = Column(Integer, primary_key = True)
	election_cycle = Column(String)
	report_date = Column(Date)
	keyword_1 = Column(String)
	spend_usd_1 = Column(Integer)
	keyword_2 = Column(String)
	spend_usd_2 = Column(Integer)
	keyword_3 = Column(String)
	spend_usd_3 = Column(Integer)
	keyword_4 = Column(String)
	spend_usd_4 = Column(Integer)
	keyword_5 = Column(String)
	spend_usd_5 = Column(Integer)
	keyword_6 = Column(String)
	spend_usd_6 = Column(Integer)
	region = Column(String)
	elections = Column(String)

def google_ad_library_create_tables(engine):
	Base.metadata.create_all(engine)
