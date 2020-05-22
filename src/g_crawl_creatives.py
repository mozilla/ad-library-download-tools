#!/usr/bin/env python3

import google_utils
import argparse

parser = argparse.ArgumentParser(
	description = "Download creatives (fulltext, images, videos) of ads in the Google Ad Library."
)
parser.add_argument("folder", help = "Folder containing a Google Ad Library download", type = str)
parser.add_argument("limit", help = "Number of ads to download", type = int, default = 1000, nargs = "?")
parser.add_argument("--headless", action = "store_true", default = False)
parser.add_argument("--shuffle", action = "store_true", default = False)
parser.add_argument("--echo", action = "store_true", default = False)
parser.add_argument("--text", help = "Download the fulltext of text ads", action = "store_true", default = False)
parser.add_argument("--images", help = "Download the images of image ads", action = "store_true", default = False)
parser.add_argument("--videos", help = "Download the videos of video ads", action = "store_true", default = False)
parser.add_argument("--screenshot", help = "Take a screenshot of the ads", action = "store_true", default = False)

# Parse command line arguments.
args = parser.parse_args()
timestamp = args.folder
limit = args.limit
headless = args.headless
shuffle = args.shuffle
echo = args.echo
download_text_ads = args.text
download_image_ads = args.images
download_video_ads = args.videos
screenshot = args.screenshot

helper = google_utils.GoogleAdCreativesDownloadHelper(timestamp, headless = headless, shuffle = shuffle, echo = echo)
if download_text_ads:
	helper.download_text_ads(limit, screenshot)
if download_image_ads:
	helper.download_image_ads(limit, screenshot)
if download_video_ads:
	helper.download_video_ads(limit, screenshot)
