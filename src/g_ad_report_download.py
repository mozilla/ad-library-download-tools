#!/usr/bin/env python3

import google_utils

helper = google_utils.GoogleAdReportDatabaseHelper()
helper.download_political_ad_report()
helper.unzip_political_ad_report()
helper.extract_all_tables()