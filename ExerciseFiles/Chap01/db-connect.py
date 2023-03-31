#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# as of 2021-04-07 bw

import sqlite3


def main():
    db = sqlite3.connect(":memory:")
    cur = db.cursor()

    cur.execute("SELECT sqlite_version()")
    version = cur.fetchone()[0]
    print(f"SQLite version {version}")

    cur.close()
    db.close()


if __name__ == "__main__":
    main()
