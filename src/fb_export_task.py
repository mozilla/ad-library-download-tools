#!/usr/bin/env python3

import facebook_utils
import argparse

DEFAULT_COUNTRY = "us"
COUNTRIES = [
	"us", "ca",
	"eu", "at", "be", "bg", "hr", "cy", "cz", "dk", "ee", "fi", "fr", "de", "gr", "hu", "ie", "it", "lv", "lt", "lu", "mt", "nl", "pl", "pt", "ro", "sk", "si", "es", "se", "uk",
	"latam", "ar", "bo", "br", "cl", "co", "ec", "fk", "gf", "gy", "py", "pe", "sr", "uy", "ve",
	"il", "in", "ua"
]

parser = argparse.ArgumentParser()
parser.add_argument("country", choices = COUNTRIES, type = str, default = DEFAULT_COUNTRY)

# Parse command line arguments.
args = parser.parse_args()
experiment_type = args.country

exports_db = facebook_utils.ExportsDBv1(experiment_type)

exports_db.export_all_ads()
