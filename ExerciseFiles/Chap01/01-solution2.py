#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# 01-solution2.py – copy table from dbms A to dbms B
# as of 2021-04-10 bw

import sqlite3
import mysql.connector as mysql


MY_HOST = 'pluto.local'
MY_USER = 'appuser'
MY_PASS = 'Spartacus'


# copy the table
def copy_table(sqlite_db, mysql_db, mode_str):
    source_db = None
    dest_db = None
    source_cur = None
    dest_cur = None
    source_str = None
    dest_str = None

    if mode_str == "mysql2sqlite":
        source_str = "mysql"
        dest_str = "sqlite"
        source_db = mysql_db
        dest_db = sqlite_db
        source_cur = source_db.cursor(prepared=True)  # mysql
        dest_cur = dest_db.cursor()
    elif mode_str == "sqlite2mysql":
        source_str = "sqlite"
        dest_str = "mysql"
        source_db = sqlite_db
        dest_db = mysql_db
        source_cur = source_db.cursor()
        dest_cur = dest_db.cursor(prepared=True)  # mysql
    else:
        print("copy setup: unexpected mode_str value")
        exit(1)

    try:
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

        # populate the source table
        print("\ninserting rows")
        source_cur.executemany("INSERT INTO copytest (a, b, c) VALUES (?, ?, ?)", create_row_data)
        source_db.commit()
        print(f"{source_cur.rowcount} rows added on {source_str}")

        print(f"{source_str} table data:")
        source_cur.execute("SELECT * FROM copytest")
        rows = source_cur.fetchall()
        for row in rows:
            print(row)

        # do the copy thing
        print(f"\ncopy the table from {source_str} to {dest_str}")
        source_cur.execute("SELECT * FROM copytest")
        sql_insert = "INSERT INTO copytest (a, b, c) VALUES (?, ?, ?)"
        for row in source_cur:
            dest_cur.execute(sql_insert, (row[1:]))
        dest_db.commit()
        print("table copied")

        print(f"query {dest_str} table:")
        dest_cur.execute("SELECT * FROM copytest")
        for row in dest_cur:
            print(row)
    except (mysql.Error, sqlite3.Error) as err:
        print(f"copy error: {err}")
        exit(1)


def main():
    # dbs and cursors
    sqlite_db = None
    sqlite_cur = None
    mysql_db = None
    mysql_cur = None

    # query the user
    mode_str = None
    while mode_str is None:
        response = input("copy table: 1) mysql to sqlite or 2) sqlite to mysql (Q to quit) ? ")
        if response == '1':
            mode_str = "mysql2sqlite"
            print("copy mysql to sqlite")
        elif response == '2':
            mode_str = "sqlite2mysql"
            print("copy sqlite to mysql")
        elif response in ('q', 'Q'):
            exit(0)

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

    # create the mysql table
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
    except mysql.Error as err:
        print(f"mysql error: {err}")
        exit(1)

    # do the copy
    copy_table(sqlite_db, mysql_db, mode_str)

    # clean up
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
