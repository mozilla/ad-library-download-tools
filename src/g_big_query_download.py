#!/usr/bin/env python3

import google_utils

#library_helper = google_utils.GoogleBigQueryDownloadHelper()
#library_helper.download_all_tables()
#timestamp = library_helper.timestamp

timestamp = "2020-05-18-12-38-38"
creatives_helper = google_utils.GoogleAdCreativeDownloadHelper(timestamp = timestamp)
creatives_helper.download_all_ad_creatives()
