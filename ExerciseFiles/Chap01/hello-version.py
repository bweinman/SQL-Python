#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# as of 2021-04-07 bw

from platform import python_version
import sqlite3
import mysql.connector as mysql


def main():
    print(f'This is python version {python_version()}')
    print(f"SQLite version {sqlite3.sqlite_version}")
    print(f"MySQL connector version {mysql.__version__}")


if __name__ == "__main__":
    main()
