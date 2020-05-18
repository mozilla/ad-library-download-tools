#!/usr/bin/env python3

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class AdCreativeContent(Base):
	__tablename__ = "ad_creative_content"
	key = Column(Integer, primary_key = True)
	ad_id = Column(String, unique = True)
	raw_html = Column(String)
	rendered_html = Column(String)
	clean_html = Column(String)

def google_ad_creatives_create_tables(engine):
	Base.metadata.create_all(engine)
