#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# as of 2021-04-07 bw

import sqlite3


def main():
    print("SQLite example")
    db = None   # satisfy the warnings monster
    cur = None

    try:
        # using SQLite's transient in-memory database
        db = sqlite3.connect(":memory:")
        cur = db.cursor()
        print("connected")

    except sqlite3.Error as e:
        print(f"could not open database: {e}")
        exit(1)

    try:
        # create a table
        sql_create = '''
            CREATE TABLE IF NOT EXISTS hello (
                id INTEGER PRIMARY KEY,
                a TEXT,
                b TEXT,
                c TEXT 
            ) 
        '''
        cur.execute(sql_create)
        print("table created")

    except sqlite3.Error as e:
        print(f"could not create table: {e}")
        exit(1)

    try:
        # insert rows into the table using executemany
        print("inserting rows")
        row_data = (
            ('one', 'two', 'three'),
            ('two', 'three', 'four'),
            ('three', 'four', 'five'),
            ('four', 'five', 'six'),
            ('five', 'six', 'seven'),
            ('six', 'seven', 'eight'),
            ('seven', 'eight', 'nine'),
            ('eight', 'nine', 'ten'),
            ('nine', 'ten', 'eleven'),
        )
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
        count = cur.rowcount
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
        count += cur.rowcount
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
        count += cur.rowcount
        print(f"{count} rows added")
        db.commit()

    except sqlite3.Error as e:
        print(f"could not insert rows: {e}")
        exit(1)

    try:
        # count rows using SELECT COUNT(*)
        cur.execute("SELECT COUNT(*) FROM hello")
        count = cur.fetchone()[0]
        print(f"there are {count} rows in the table")

        # get column names from SQLite meta-data table_info
        cur.execute("PRAGMA table_info(hello);")
        row = cur.fetchall()
        colnames = [r[1] for r in row]
        print(f"column names are: {colnames}")

        # fetch rows using iterator
        print('\nusing iterator')
        cur.execute("SELECT * FROM hello LIMIT 5")
        for row in cur:
            print(row)

        # fetch rows using row_factory
        print('\nusing row_factory')
        cur.execute("SELECT * FROM hello LIMIT 5")
        cur.row_factory = sqlite3.Row
        for row in cur:
            print(f"as tuple: {tuple(row)}, as dict: id:{row['id']} a:{row['a']}, b:{row['b']}, c:{row['c']}")

        cur.row_factory = None  # reset row factory

        # fetch rows in groups of 5 using fetchmany
        print('\ngroups of 5 using fetchmany')
        cur.execute("SELECT * FROM hello")
        rows = cur.fetchmany(5)
        while rows:
            for r in rows:
                print(r)
            print("====== ====== ======")
            rows = cur.fetchmany(5)

        # drop table and close the database
        print('\ndrop table and close connection')
        cur.execute("DROP TABLE IF EXISTS hello")  # cleanup if db is not :memory:
        cur.close()
        db.close()

    except sqlite3.Error as e:
        print(f"sqlite3 error: {e}")
        exit(1)


if __name__ == "__main__":
    main()
