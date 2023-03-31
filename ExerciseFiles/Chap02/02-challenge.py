#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# 01-solution2.py – copy table from dbms A to dbms B
# as of 2021-04-07 bw

from BWDB import BWDB, BWErr


GLOBALS = {}


def connect():
    try:
        db = BWDB(dbms='sqlite', database='../db/scratch.db', table='domains')
        print(f"BWDB version {db.version()}")
        print(f"dbms is {db.dbms}")
    except BWErr as err:
        db = None
        print(f"Error: {err}")
        exit(1)

    GLOBALS['db'] = db
    return db


def do_menu():
    while True:
        menu = (
            "A) Add domain",
            "F) Find domain",
            "E) Edit domain",
            "L) List domains",
            "D) Delete domain",
            "X) Drop table and exit",
            "Q) Quit",
        )
        print()
        for s in menu:
            print(s)
        response = input("Select an action or Q to quit > ").upper()
        if len(response) != 1:
            print("\nInput too long or empty")
            continue
        elif response in 'AFELDXQ':
            break
        else:
            print("\nInvalid response")
            continue
    return response


def jump(response):
    if response == 'A':
        add_domain()
    elif response == 'F':
        find_domain()
    elif response == 'E':
        edit_domain()
    elif response == 'L':
        list_domains()
    elif response == 'D':
        delete_domain()
    elif response == 'X':
        drop_db()
    else:
        print("jump: invalid argument")
    return


def add_domain():
    print("Add domain")


def find_domain():
    print("Find domain")


def edit_domain():
    print("Edit domain")


def list_domains():
    print("List Domains")


def delete_domain():
    print("Delete Domain")


def drop_db():
    print("Drop the table and exit")


def main():
    connect()
    db = GLOBALS['db']

    # create table if not exists
    create_table_statement = '''
        CREATE TABLE IF NOT EXISTS domains (
                id INTEGER PRIMARY KEY,
                domain VARCHAR(127) NOT NULL,
                description VARCHAR(255)
        )
    '''
    try:
        db.sql_do(create_table_statement)
    except BWErr as err:
        print(f"cannot create table: {err}")
        exit(1)

    db.table = 'domains'
    # menu
    while True:
        response = do_menu()
        if response == 'Q':
            print("Quitting.")
            exit(0)
        else:
            print()  # blank line
            jump(response)


if __name__ == "__main__":
    main()
