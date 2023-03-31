#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# as of 2021-04-07 bw

import mysql.connector as mysql


MY_HOST = 'pluto.local'
MY_USER = 'appuser'
MY_PASS = 'Spartacus'


def main():
    print("MySQL example")
    db = None   # satisfy the warnings monster
    cur = None

    try:
        db = mysql.connect(host=MY_HOST, user=MY_USER, password=MY_PASS, database='scratch')
        cur = db.cursor(prepared=True)
        print("connected")

    except mysql.Error as err:
        print(f"could not connect to MySQL: {err})")
        exit(1)

    try:
        cur.execute("DROP TABLE IF EXISTS hello")

        # create a table
        sql_create = '''
            CREATE TABLE IF NOT EXISTS hello (
                id SERIAL PRIMARY KEY,
                a VARCHAR(16),
                b VARCHAR(16),
                c VARCHAR(16)
            )
        '''
        cur.execute(sql_create)
        print("table created")

    except mysql.Error as err:
        print(f"could not create table: {err})")
        exit(1)

    try:
        # insert rows into the table using executemany
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
        print("inserting rows")
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
        count = cur.rowcount
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
        count += cur.rowcount
        cur.executemany("INSERT INTO hello (a, b, c) VALUES (?, ?, ?)", row_data)
        count += cur.rowcount
        print(f"{count} rows added")
        db.commit()

    except mysql.Error as err:
        print(f"could not add rows: {err})")
        exit(1)

    try:
        # count rows using SELECT COUNT(*)
        cur.execute("SELECT COUNT(*) FROM hello")
        count = cur.fetchone()[0]
        print(f"there are {count} rows in the table")

        # get column names by selecting one row
        cur.execute("SELECT * FROM hello LIMIT 1")
        cur.fetchall()
        colnames = cur.column_names
        print(f"column names are: {colnames}")

        # fetch rows using iterator
        print('\nusing iterator')
        cur.execute("SELECT * FROM hello LIMIT 5")
        for row in cur:
            print(row)

        # fetch rows using dictionary workaround
        print('\ndictionary workaround')
        cur.execute("SELECT * FROM hello LIMIT 5")
        for row in cur:
            rd = dict(zip(colnames, row))
            print(f"as tuple: {tuple(row)}, as dict: id:{rd['id']} a:{rd['a']}, b:{rd['b']}, c:{rd['c']}")

        # fetch rows in groups of 5 using fetchmany
        print('\ngroups of 5 using fetchmany')
        cur.execute("SELECT * FROM hello")
        rows = cur.fetchmany(5)
        while rows:
            for r in rows:
                print(r)
            print("====== ====== ======")
            rows = cur.fetchmany(5)

        # drop table and close connection
        print('\ndrop table and close connection')
        cur.execute("DROP TABLE IF EXISTS hello")
        cur.close()
        db.close()

    except mysql.Error as err:
        print(f"mysql error ({err})")
        exit(1)


if __name__ == "__main__":
    main()
