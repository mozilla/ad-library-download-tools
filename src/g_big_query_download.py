#!/usr/bin/env python3

import google_utils

#library_helper = google_utils.GoogleBigQueryDownloadHelper()
#library_helper.download_all_tables()
#timestamp = library_helper.timestamp

timestamp = "2020-05-18-15-24-55"
creatives_helper = google_utils.GoogleAdCreativesDownloadHelper(timestamp = timestamp, shuffle = True, headless = True)
creatives_helper.download_all_ad_creatives()
