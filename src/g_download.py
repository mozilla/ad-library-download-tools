#!/usr/bin/env python3

import google_utils

gc_helper = google_utils.GoogleCloudHelper()
#gc_helper.download_all_tables()
gc_helper.download_table("bigquery-public-data.google_political_ads.geo_spend")
