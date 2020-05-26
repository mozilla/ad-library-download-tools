#!/usr/bin/env python3

import google_utils
import arg_parse

parser = argparse.ArgumentParser(
	usage = "Download Google Ad Library.",
	description = "This script downloads the Google Ad Library via the web using the Google Ad Library Report.",
)

helper = google_utils.GoogleAdReportDownloadHelper()
helper.download_political_ad_report()
helper.unzip_political_ad_report()
helper.extract_all_tables()
print()
print("Download folder = {:s}".format(helper.timestamp))
print()
