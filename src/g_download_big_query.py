#!/usr/bin/env python3

import google_utils
import arg_parse

parser = argparse.ArgumentParser(
	usage = "Download Google Ad Library.",
	description = "This script downloads the Google Ad Library using the GoogleCloud BigQuery service, and requires a GoogleCloud service account key.",
)

helper = google_utils.GoogleBigQueryDownloadHelper()
helper.download_all_tables()
print()
print("Download folder = {:s}".format(helper.timestamp))
print()
