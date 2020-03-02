#!/usr/bin/env python3

import facebook_utils

import argparse

parser = argparse.ArgumentParser(
	usage = "Generate a long-lived user access token.",
	description = "This script calls the Facebook OAuth endpoint, and generates a long-lived user access token. A long-lived token can be used to access the Facebook Graph API for up to three months. You will need your Facebook app id, app secret, and a short-lived user access token in order to generate a long-lived token. You can set your Facebook app id an app secret using the scripts 'fb_set_app_id.py' and 'fb_set_app_secret.py'. You can generate a short-lived token using the Facebook Graph API Explorer at https://developers.facebook.com/tools/explorer"
)
parser.add_argument("short_lived_token", help = "short-lived user access token", type = str)
args = parser.parse_args()

tokens_manager = facebook_utils.TokensManager()
tokens_manager.generate_user_access_token(args.short_lived_token)
