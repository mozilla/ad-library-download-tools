#!/usr/bin/env python3

from common import Constants

import configparser
from datetime import datetime
import json
import os
import requests
import urllib.parse

# Configuration files
APP_CONFIG_FILENAME = os.path.join(Constants.PREF_PATH, "facebook_app.ini")
TOKEN_CONFIG_FILENAME = os.path.join(Constants.PREF_PATH, "facebook_tokens.ini")

# App configurations
APP_SECTION = "facebook_app"
APP_ID_OPTION = "app_id"
APP_SECRET_OPTION = "app_secret"

# Token configurations
LATEST_SECTION = "latest"
LOG_SECTION_PREFIX = "log"
TIMESTAMP_OPTION = "timestamp"
SHORT_LIVED_USER_ACCESS_TOKEN_OPTION = "short_lived_user_access_token"
LONG_LIVED_USER_ACCESS_TOKEN_OPTION = "long_lived_user_access_token"

# Facebook OAuth specs
URL_BASE = "https://graph.facebook.com/oauth/access_token"
FB_EXCHANGE_TOKEN_GRANT_TYPE = "fb_exchange_token"

class TokenManager:
	def __init__(self, verbose = True):
		assert isinstance(verbose, bool)
		self.verbose = verbose
		self._init_configs()

	def _init_configs(self):
		for filename in [APP_CONFIG_FILENAME, TOKEN_CONFIG_FILENAME]:
			if not os.path.exists(filename):
				config = configparser.ConfigParser()
				with open(filename, "w") as f:
					config.write(f)
				if self.verbose:
					print("[TokenManager] Created file: {}".format(filename))

	def _write_app_id(self, app_id):
		filename = APP_CONFIG_FILENAME
		config = configparser.ConfigParser()
		config.read(filename)
		if not config.has_section(APP_SECTION):
			config.add_section(APP_SECTION)
		config.set(APP_SECTION, APP_ID_OPTION, app_id)
		with open(filename, 'w') as f:
			config.write(f)
		if self.verbose:
			print("[TokenManager] Wrote app id to file: {}".format(filename))

	def _write_app_secret(self, app_secret):
		filename = APP_CONFIG_FILENAME
		config = configparser.ConfigParser()
		config.read(filename)
		if not config.has_section(APP_SECTION):
			config.add_section(APP_SECTION)
		config.set(APP_SECTION, APP_SECRET_OPTION, app_secret)
		with open(filename, 'w') as f:
			config.write(f)
		if self.verbose:
			print("[TokenManager] Wrote app secret to file: {}".format(filename))

	def _read_app_id(self):
		filename = APP_CONFIG_FILENAME
		try:
			config = configparser.ConfigParser()
			config.read(filename)
			app_id = config.get(APP_SECTION, APP_ID_OPTION)
		except (configparser.NoSectionError, configparser.NoOptionError) as e:
			print()
			print("[TokenManager] [ERROR] Cannot read app id from file:", filename)
			print()
			raise
		if self.verbose:
			print("[TokenManager] Read app id from file: {}".format(filename))
		return app_id

	def _read_app_secret(self):
		filename = APP_CONFIG_FILENAME
		try:
			config = configparser.ConfigParser()
			config.read(filename)
			app_secret = config.get(APP_SECTION, APP_SECRET_OPTION)
		except (configparser.NoSectionError, configparser.NoOptionError) as e:
			print()
			print("[TokenManager] [ERROR] Cannot read app secret from file:", filename)
			print()
			raise
		if self.verbose:
			print("[TokenManager] Read app secret from file: {}".format(filename))
		return app_secret

	def _write_user_tokens(self, short_lived_user_access_token, long_lived_user_access_token):
		filename = TOKEN_CONFIG_FILENAME
		config = configparser.ConfigParser()
		config.read(filename)

		# Create a new log section
		now = datetime.now()
		timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
		log_section = "{:s}_{:s}".format(LOG_SECTION_PREFIX, now.strftime("%Y_%m_%d_%H_%M_%S"))

		# Write user tokens to a newly-created log section of the config file
		config.add_section(log_section)
		config.set(log_section, TIMESTAMP_OPTION, timestamp)
		config.set(log_section, SHORT_LIVED_USER_ACCESS_TOKEN_OPTION, short_lived_user_access_token)
		config.set(log_section, LONG_LIVED_USER_ACCESS_TOKEN_OPTION, long_lived_user_access_token)

		# Write user tokens to the latest section of the config file
		if not config.has_section(LATEST_SECTION):
			config.add_section(LATEST_SECTION)
		config.set(LATEST_SECTION, TIMESTAMP_OPTION, timestamp)
		config.set(LATEST_SECTION, SHORT_LIVED_USER_ACCESS_TOKEN_OPTION, short_lived_user_access_token)
		config.set(LATEST_SECTION, LONG_LIVED_USER_ACCESS_TOKEN_OPTION, long_lived_user_access_token)

		# Write to config file
		with open(filename, "w") as f:
			config.write(f)
		if self.verbose:
			print("[TokenManager] Wrote user access tokens to file: {}".format(filename))

	def _read_latest_user_token(self):
		filename = TOKEN_CONFIG_FILENAME
		try:
			config = configparser.ConfigParser()
			config.read(filename)
			app_secret = config.get(LATEST_SECTION, LONG_LIVED_USER_ACCESS_TOKEN_OPTION)
		except (configparser.NoSectionError, configparser.NoOptionError) as e:
			print()
			print("[TokenManager] [ERROR] Cannot read the latest long-lived user access token from file:", filename)
			print()
			raise
		if self.verbose:
			print("[TokenManager] Read the latest long-lived user access token from file: {}".format(filename))
		return app_secret

	def _generate_long_lived_token(self, short_lived_user_access_token):
		app_id = self._read_app_id()
		app_secret = self._read_app_secret()
		data = {
		    "grant_type": FB_EXCHANGE_TOKEN_GRANT_TYPE,
			"client_id": app_id,
			"client_secret": app_secret,
			"fb_exchange_token": short_lived_user_access_token
		}

		# Exchange short-lived user access token for a long-lived user access token
		url = "{}?{}".format(URL_BASE, urllib.parse.urlencode(data))
		try:
			r = requests.get(url)
			results = r.json()
			if "error" in results:
				print()
				print("[TokenManager] [ERROR] Received an error message from server: ", URL_BASE)
				print(json.dumps(results, indent = 2, sort_keys = True))
				print()
				raise
			long_lived_user_access_token = results["access_token"]
		except requests.exceptions.ConnectionError:
			print()
			print("[TokenManager] [ERROR] Cannot connect to server:", URL_BASE)
			print()
			raise

		return long_lived_user_access_token

	def set_app_id(self, app_id):
		assert isinstance(app_id, str)
		self._write_app_id(app_id)

	def set_app_secret(self, app_secret):
		assert isinstance(app_secret, str)
		self._write_app_secret(app_secret)

	def generate_user_access_token(self, short_lived_user_access_token):
		assert isinstance(short_lived_user_access_token, str)
		long_lived_user_access_token = self._generate_long_lived_token(short_lived_user_access_token)
		self._write_user_tokens(short_lived_user_access_token, long_lived_user_access_token)

	def get_user_access_token(self):
		long_lived_user_access_token = self._read_latest_user_token()
		return long_lived_user_access_token

