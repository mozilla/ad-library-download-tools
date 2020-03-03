#!/usr/bin/env python

import facebook_utils

token_manager = facebook_utils.TokenManager()
access_token = token_manager.get_user_access_token()

print("[access_token]")
print(access_token)
print()

queue_manager = facebook_utils.QueueManager(db_folder = "../db/test")
next_task = queue_manager.get_next_task()

print("[next_task]")
print(next_task)
print()

facebook_api = facebook_utils.FacebookAPI()
url = facebook_api.get_url(next_task, access_token)

print("[url]")
print(url)
print()

response = facebook_api.search(url)

print("[response]")
print(response)
print()

