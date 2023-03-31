#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# simple python web server
# as of 2021-04-07 bw

from http.server import HTTPServer, CGIHTTPRequestHandler


class Handler(CGIHTTPRequestHandler):
    cgi_directories = ["/", "/jurl"]


PORT = 9999

httpd = HTTPServer(("", PORT), Handler)
print("serving at port", PORT)
httpd.serve_forever()
