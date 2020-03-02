#!/usr/bin/env python3

import argparse
import facebook_utils

parser = argparse.ArgumentParser(
	usage = "Save your Facebook app id to a local file.",
	description = "This script saves your Facebook app id, which is needed for generating a long-lived user access token. The app id is saved to a local file and not used in other ways.  You can look up your Facebook app id in your Facebook for Developer dashboard under 'Settings > Basic'."
)
parser.add_argument("app_id", help = "Facebook add id", type = str)
args = parser.parse_args()

tokensManager = facebook_utils.TokensManager()
tokensManager.set_app_id(args.app_id)
