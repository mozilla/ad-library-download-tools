#!/usr/bin/env python3

import facebook_utils

import argparse

parser = argparse.ArgumentParser(
	usage = "Save your Facebook app secret to a local file.",
	description = "This script saves your Facebook app secret, which is needed for generating a long-lived user access token. The app secret is saved to a local file and not used in other ways. You can look up your Facebook app secret in your Facebook for Developer dashboard under 'Settings > Basic'."
)
parser.add_argument("app_secret", help = "Facebook add secret", type = str)
args = parser.parse_args()

token_manager = facebook_utils.TokenManager()
token_manager.set_app_secret(args.app_secret)
