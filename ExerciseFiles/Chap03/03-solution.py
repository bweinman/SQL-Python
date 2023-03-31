#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# 02-solution.py – a simple cli crud
# as of 2021-04-07 bw

from jurl.BWDB import BWDB, BWErr


GLOBALS = {}


def connect():
    try:
        db = BWDB(dbms='sqlite', database='./data/jurl.db')
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
        add_rec()
    elif response == 'F':
        find_rec()
    elif response == 'E':
        edit_rec()
    elif response == 'L':
        list_recs()
    elif response == 'D':
        delete_rec()
    else:
        print("jump: invalid argument")


def add_rec():
    print("Add record")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("add_domain: no db object")
    shortname = input("Short name  > ")
    target_url = input("Target URL > ")
    count = db.add_row([shortname, target_url])
    if count < 1:
        raise BWErr("unable to add record")
    row_id = db.lastrowid()
    row = db.get_row(row_id)
    print(f"row added: {row}")


def find_rec(**kwargs):
    if 'noprompt' not in kwargs:
        print("Find record")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("find_rec: no db object")
    shortname = input("Short name > ")
    if len(shortname) == 0:
        return
    row_id = db.find_row('shortURL', shortname)
    if row_id:
        row = db.get_row(row_id)
        print(f"found: {row}")
        return row_id
    else:
        print("row not found.")
        return None


def edit_rec():
    print("Edit record")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("edit_rec: no db object")
    row_id = find_rec(noprompt=True)
    if row_id is None:
        return
    target_url = input("Target URL (leave blank to cancel) > ")
    if len(target_url) == 0:
        print("Canceled.")
        return
    else:
        db.update_row(row_id, {'targetURL': target_url})
        row = db.get_row(row_id)
        print(f"Updated row is {row}")


def list_recs():
    print("List records")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("list_recs: no db object")
    for row in db.get_rows():
        print(row)


def delete_rec():
    print("Delete record")
    db = GLOBALS['db']
    if db is None:
        raise BWErr("delete_rec: no db object")
    row_id = find_rec(noprompt=True)
    if row_id:
        yn = input("Delete row? (Y/N) > ").upper()
        if yn == 'Y':
            db.del_row(row_id)
            print("Deleted.")
        else:
            print("Not deleted.")


def main():
    connect()
    db = GLOBALS['db']

    # create table if not exists
    create_table_statement = '''
        CREATE TABLE IF NOT EXISTS jurl (
            id integer PRIMARY KEY,
            shortURL VARCHAR(32) UNIQUE NOT NULL,
            targetURL VARCHAR(128) NOT NULL
        )
    '''
    try:
        if not db.have_table('jurl'):
            db.sql_do(create_table_statement)
    except BWErr as err:
        print(f"cannot create table: {err}")
        exit(1)

    db.table = 'jurl'

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
