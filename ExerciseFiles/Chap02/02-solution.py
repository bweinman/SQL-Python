#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# 02-solution.py – a simple cli crud
# as of 2021-04-07 bw

from BWDB import BWDB, BWErr


GLOBALS = {}


def connect():
    try:
        db = BWDB(dbms='sqlite', database='../db/scratch.db')
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


def add_domain():
    print("Add domain")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("add_domain: no db object")
    domain = input("Domain name > ")
    description = input("Description > ")
    count = db.add_row([domain, description])
    if count < 1:
        raise BWErr("unable to add domain")
    row_id = db.lastrowid()
    row = db.get_row(row_id)
    print(f"row added: {row}")


def find_domain(**kwargs):
    if 'noprompt' not in kwargs:
        print("Find domain")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("find_domain: no db object")
    domain = input("Domain name > ")
    if len(domain) == 0:
        return
    row_id = db.find_row('domain', domain)
    if row_id:
        row = db.get_row(row_id)
        print(f"found: {row}")
        return row_id
    else:
        print("row not found.")
        return None


def edit_domain():
    print("Edit domain")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("edit_domain: no db object")
    row_id = find_domain(noprompt=True)
    if row_id is None:
        return
    description = input("Description (leave blank to cancel) > ")
    if len(description) == 0:
        print("Canceled.")
        return
    else:
        db.update_row(row_id, {'description': description})
        row = db.get_row(row_id)
        print(f"Updated row is {row}")


def list_domains():
    db = GLOBALS['db']
    if db is None:
        raise BWErr("list_domains: no db object")
    print(f"Listing {db.count_rows()} domain(s)")
    for row in db.get_rows():
        print(row)


def delete_domain():
    print("Delete Domain")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("delete_domain: no db object")
    row_id = find_domain(noprompt=True)
    if row_id:
        yn = input("Delete row? (Y/N) > ").upper()
        if yn == 'Y':
            db.del_row(row_id)
            print("Deleted.")
        else:
            print("Not deleted.")


def drop_db():
    print("Drop the table and exit")
    try:
        db = GLOBALS['db']
        if db is None:
            raise BWErr("drop_db: no db object")
        db.sql_do("DROP TABLE IF EXISTS domains")
        exit(0)

    except BWErr as err:
        print(f"Error: {err}")
        exit(1)


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
        if not db.have_table('domains'):
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
