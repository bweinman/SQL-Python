#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# 01-solution1.py – copy table from mysql to sqlite3
# as of 2021-04-10 bw

import sqlite3
import mysql.connector as mysql


MY_HOST = 'pluto.local'
MY_USER = 'appuser'
MY_PASS = 'Spartacus'


def main():
    # dbs and cursors
    sqlite_db = None
    sqlite_cur = None
    mysql_db = None
    mysql_cur = None

    # connect to sqlite
    try:
        sqlite_db = sqlite3.connect("../db/scratch.db")
        sqlite_cur = sqlite_db.cursor()
        sqlite_cur.execute("SELECT sqlite_version()")
        print(f"SQLite version {sqlite_cur.fetchone()[0]}")
    except sqlite3.Error as err:
        print(f"sqlite error: {err}")
        exit(1)

    # connect to mysql
    try:
        mysql_db = mysql.connect(host=MY_HOST, user=MY_USER, password=MY_PASS, database='scratch')
        mysql_cur = mysql_db.cursor(prepared=True)
        mysql_cur.execute("SHOW VARIABLES WHERE variable_name = 'version'")
        print(f"MySQL server version: {mysql_cur.fetchone()[1]}")
    except mysql.Error as err:
        print(f"mysql error: {err}")
        exit(1)

    # create and populate mysql table
    try:
        print("create the mysql table")
        mysql_cur.execute("DROP TABLE IF EXISTS copytest")
        mysql_create = '''
            CREATE TABLE IF NOT EXISTS copytest (
                id SERIAL PRIMARY KEY,
                a INT,
                b VARCHAR(16),
                c VARCHAR(16)
            )
        '''
        mysql_cur.execute(mysql_create)
        create_row_data = (
            (1, 'two', 'three'),
            (2, 'three', 'four'),
            (3, 'four', 'three'),
            (4, 'five', 'four'),
            (5, 'six', 'seven'),
            (6, 'seven', 'eight'),
            (7, 'eight', 'nine'),
            (8, 'nine', 'ten'),
            (9, 'ten', 'eleven'),
            (10, 'eleven', 'twelve')
        )
        print("inserting rows")
        mysql_cur.executemany("INSERT INTO copytest (a, b, c) VALUES (?, ?, ?)", create_row_data)
        mysql_db.commit()
        print(f"{mysql_cur.rowcount} rows added")

        print("mysql table data:")
        mysql_cur.execute("SELECT * FROM copytest")
        for row in mysql_cur:
            print(row)
    except mysql.Error as err:
        print(f"mysql error: {err}")
        exit(1)

    # create the sqlite table
    try:
        print("\ncreate the sqlite table")
        sqlite_cur.execute("DROP TABLE IF EXISTS copytest")
        sqlite_create = '''
            CREATE TABLE IF NOT EXISTS copytest (
                id INTEGER PRIMARY KEY,
                a INTEGER,
                b TEXT,
                c TEXT
            )
        '''
        sqlite_cur.execute(sqlite_create)
    except sqlite3.Error as err:
        print(f"sqlite error: {err}")
        exit(1)

    # copy from mysql to sqlite
    try:
        print("copy the table from mysql to sqlite")
        mysql_cur.execute("SELECT * FROM copytest")
        sql_insert = "INSERT INTO copytest (a, b, c) VALUES (?, ?, ?)"
        for row in mysql_cur:
            sqlite_cur.execute(sql_insert, (row[1:]))
        sqlite_db.commit()

        print("\nquery sqlite table:")
        sqlite_cur.execute("SELECT * FROM copytest")
        for row in sqlite_cur:
            print(row)
    except (mysql.Error, sqlite3.Error) as err:
        print(f"copy error: {err}")
        exit(1)

    # cleanup
    try:
        print("drop the tables and close")
        mysql_cur.execute("DROP TABLE IF EXISTS copytest")
        sqlite_cur.execute("DROP TABLE IF EXISTS copytest")
        mysql_cur.close()
        mysql_db.close()
        sqlite_cur.close()
        sqlite_db.close()
    except (mysql.Error, sqlite3.Error) as err:
        print(f"close error: {err}")
        exit(1)


if __name__ == "__main__":
    main()
