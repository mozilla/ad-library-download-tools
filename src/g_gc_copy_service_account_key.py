#!/usr/bin/env python3

import google_utils
import argparse
import os
import shutil
import stat

parser = argparse.ArgumentParser(
	usage = "Copy your Google Service Account key to prefs folder."
)
parser.add_argument("service_account_key", help = "Google Service Account Key (as a JSON file)", type = str)
args = parser.parse_args()

helper = google_utils.GoogleBigQueryDownloadHelper()
src_path = args.service_account_key
dest_path = helper.get_service_account_key_path()

print("Copying from {} to {}...".format(src_path, dest_path))
shutil.copyfile(src_path, dest_path)

print("Setting file permissions to 600...")
os.chmod(dest_path, stat.S_IRUSR | stat.S_IWUSR)
