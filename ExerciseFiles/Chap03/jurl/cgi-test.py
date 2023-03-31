#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# cgi-test.py by Bill Weinman
# Python version of CGI testing script
# as of 2021-03-15

from os import environ
from platform import python_version

version = "6.0.1 py"

for s in (
    "Content-type: text/plain",     # CGI header
    "",                             # blank line terminates headers
    f"BW Test version: {version}",
    "Copyright 2021 BHG [bw.org]",
    f"Python version {python_version()}"
):
    print(s)

print("\nEnvironment Variables:\n=================")
for k in ("GATEWAY_INTERFACE", "HTTP_ACCEPT", "HTTP_USER_AGENT",
          "PATH_INFO", "QUERY_STRING", "REMOTE_ADDR", "SCRIPT_NAME",
          "SERVER_PROTOCOL", "SERVER_SOFTWARE"):
    if k in environ.keys():
        v = environ[k]
        print(f"{k} [{v}]")

# show all env
# print("\nFull Environment Variables:\n=================")
# for k in environ.keys():
#     v = environ[k]
#     print(f"{k} [{v}]")
