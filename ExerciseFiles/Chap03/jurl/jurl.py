#!/usr/bin/env python3
#
# jurl.py by Bill Weinman <https://bw.org/contact/>
# Jump to URL (a private short URL service)
# Copyright (c) 2010-2021 The BearHeart Group, LLC
# updated 2021-04-16 for Using SQL With Python
#

from BWDB import BWDB
from BWCGI import BWCGI
from BWConfig import ConfigFile
import sys
import os

g = dict(
    # config_file='./db.conf',
    config_file='./jurl/db.conf',
    table_name='jurl'
)

default_url = 'https://bw.org/error'


def main():
    config = ConfigFile(g['config_file']).recs()
    db = BWDB(dbms='sqlite', database=config['db'], table=g['table_name'])
    cgi = BWCGI()
    cgi_vars = cgi.vars()

    if 'u' in cgi_vars:
        key = cgi_vars.getfirst('u')
    elif 'PATH_INFO' in os.environ:
        key = os.environ['PATH_INFO']
    elif len(sys.argv) > 1:
        key = sys.argv[1]
    else:
        redirect(default_url)
        return 0

    if key.startswith('/'):
        key = key[1:]
    try:
        target = db.sql_query_value(f"SELECT targetURL FROM {g['table_name']} WHERE shortURL = ?", [key])
    except TypeError:
        redirect(default_url)
    else:
        redirect(target)


def redirect(u):
    print("content-type: text/plain", end='\r\n\r\n')   # comment for run / uncomment for debugging
    print("Status: 307 Temporary Redirect", end='\r\n')
    print(f"Location: {u}", end='\r\n\r\n')


def error(e):
    print(e)
    exit(0)


if __name__ == "__main__":
    main()
