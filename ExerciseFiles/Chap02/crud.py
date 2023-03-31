#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# as of 2021-04-07 bw

from BWDB import BWDB, BWErr


MY_HOST = 'pluto.local'
MY_USER = 'appuser'
MY_PASS = 'Spartacus'


def main():
    try:
        # db = BWDB(dbms='sqlite', database='../db/scratch.db')
        db = BWDB(dbms='mysql', host=MY_HOST, user=MY_USER, password=MY_PASS,
                  database='scratch')

        print(f"BWDB version {db.version()}")
        print(f"dbms is {db.dbms}\n")

        # start clean
        db.sql_do("DROP TABLE IF EXISTS temp")

        print("create a table")
        if db.dbms == "sqlite":
            create_table = """
                CREATE TABLE IF NOT EXISTS temp (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT
                )
            """
        elif db.dbms == "mysql":
            create_table = """
                CREATE TABLE IF NOT EXISTS temp (
                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(128) NOT NULL,
                    description VARCHAR(128)
                )
            """
        else:
            raise BWErr("create table: unknown dbms")

        # create and set the table
        db.sql_do(create_table)
        db.table = "temp"
        print(f"table columns: {db.column_names()}\n")

        print("populate table")
        insert_rows = (
            ("Jimi Hendrix", "Guitar"),
            ("Miles Davis", "Trumpet"),
            ("Billy Cobham", "Drums"),
            ("Charlie Bird", "Saxophone"),
            ("Oscar Peterson", "Piano"),
            ("Marcus Miller", "Bass"),
        )

        for row in insert_rows:
            db.add_row_nocommit(row)
        db.commit()
        print(f"added {len(insert_rows)} rows")

        for row in db.get_rows():
            print(row)

        print()
        print("find more than one row (%s%)")
        row_ids = db.find_rows("name", "%s%")
        print(f"found {len(row_ids)} rows")
        for row_id in row_ids:
            print(db.get_row(row_id))

        print()
        print("search for %Bird%")
        row_id = db.find_row("name", "%Bird%")
        if row_id > 0:
            print(f"found row {row_id}")
            print(db.get_row(row_id))

        print()
        print(f"update row {row_id}")
        numrows = db.update_row(row_id, {'name': 'The Bird'})
        print(f"{numrows} row(s) modified")
        print(db.get_row(row_id))

        print()
        print("add a row")
        numrows = db.add_row(["Bob Dylan", "Harmonica"])
        row_id = db.lastrowid()
        print(f"{numrows} row added (row {row_id})")
        print(db.get_row(row_id))

        print()
        print("delete a row (Cobham)")
        row_id = db.find_row("name", "%Cobham%")
        if row_id > 0:
            print(f"deleting row {row_id}")
            numrows = db.del_row(row_id)
            print(f"{numrows} row(s) deleted")

        print()
        print("print remaining rows")
        for row in db.get_rows():
            print(row)

        # add more rows to test paging
        print()
        print("add more rows")
        for row in insert_rows:
            numrows = db.add_row_nocommit(row)
        for row in insert_rows:
            numrows += db.add_row_nocommit(row)
        for row in insert_rows:
            numrows += db.add_row_nocommit(row)
        db.commit()
        print(f"added {numrows} rows")

        print()
        print("page through rows")
        offset = 0
        limit = 5
        while True:
            count = 0
            for row in db.get_rows_limit(limit, offset):
                print(row)
                count += 1
            if count == 0:  # no rows left
                break
            else:
                print("=====")
                offset += 5

        print()
        print("change table to item")
        db.table = "item"
        for row in db.get_rows():
            print(row)

        print()
        print("change table to temp")
        db.table = "temp"
        for row in db.get_rows_limit(6):
            print(row)

        # cleanup
        print()
        print("cleanup: drop table temp")
        db.sql_do("DROP TABLE IF EXISTS temp")
        print("done.")

    except BWErr as err:
        print(f"Error: {err}")
        exit(1)


if __name__ == "__main__":
    main()
