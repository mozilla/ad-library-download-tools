#!/usr/bin/env python3

import google_utils

helper = google_utils.GoogleBigQueryDownloadHelper()
helper.download_all_tables()
