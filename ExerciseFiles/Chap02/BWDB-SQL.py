#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# BWDB-SQL.py as of 2021-04-11 bw


# module version
__version__ = "3.1.3"

# import sqlite3
try:
    import sqlite3
    have_sqlite3 = True
except ImportError:
    sqlite3 = None
    have_sqlite3 = False

# import mysql
try:
    import mysql.connector as mysql
    have_mysql = True
except ImportError:
    mysql = None
    have_mysql = False


class BWErr(Exception):
    """Simple Error class for BWDB"""
    def __init__(self, message):
        self.message = message
        super.__init__(self.message)


class BWDB:
    def __init__(self, **kwargs):
        self._db = None
        self._cur = None
        self._dbms = None
        self._database = None

        # populate simple parameters first
        if 'user' in kwargs:
            self._user = kwargs['user']
        else:
            self._user = None

        if 'password' in kwargs:
            self._password = kwargs['password']
        else:
            self._password = None

        if 'host' in kwargs:
            self._host = kwargs['host']
        else:
            self._host = None

        # populate properties
        if 'dbms' in kwargs:
            self.dbms = kwargs['dbms']

        if 'database' in kwargs:
            self.database = kwargs['database']

    # property setters/getters
    def get_dbms(self):
        return self._dbms

    def set_dbms(self, dbms_str):
        if dbms_str == 'mysql':
            if have_mysql:
                self._dbms = dbms_str
            else:
                raise BWErr('mysql not available')
        elif dbms_str == 'sqlite':
            if have_sqlite3:
                self._dbms = dbms_str
            else:
                raise BWErr('sqlite not available')
        else:
            raise BWErr('set_dbms: invalid dbms_str specified')

    def get_database(self):
        return self._database

    def set_database(self, database):
        self._database = database
        if self._cur:
            self._cur.close()
        if self._db:
            self._db.close()

        self._database = database
        if self._dbms == 'sqlite':
            self._db = sqlite3.connect(self._database)
            if self._db is None:
                raise BWErr('set_database: failed to open sqlite database')
            else:
                self._cur = self._db.cursor()
        elif self._dbms == 'mysql':
            self._db = mysql.connect(user=self._user, password=self._password,
                                     host=self._host, database=self._database)
            if self._db is None:
                raise BWErr('set_database: failed to connect to mysql')
            else:
                self._cur = self._db.cursor(prepared=True)
        else:
            raise BWErr('set_database: unknown _dbms')

    def get_cursor(self):
        return self._cur

    # properties
    dbms = property(fget=get_dbms, fset=set_dbms)
    database = property(fget=get_database, fset=set_database)
    cursor = property(fget=get_cursor)

    # sql methods =====
    def sql_do_nocommit(self, sql, parms=()):
        """Execute an SQL statement"""
        self._cur.execute(sql, parms)
        return self._cur.rowcount

    def sql_do(self, sql, parms=()):
        """Execute an SQL statement"""
        self.sql_do_nocommit(sql, parms)
        self.commit()
        return self._cur.rowcount

    def sql_do_many_nocommit(self, sql, parms=()):
        """Execute an SQL statement over set of data"""
        self._cur.executemany(sql, parms)
        return self._cur.rowcount

    def sql_do_many(self, sql, parms=()):
        """Execute an SQL statement over set of data"""
        self._cur.executemany(sql, parms)
        self.commit()
        return self._cur.rowcount

    def sql_query(self, sql, parms=()):
        self._cur.execute(sql, parms)
        for row in self._cur:
            yield row

    def sql_query_row(self, sql, parms=()):
        self._cur.execute(sql, parms)
        row = self._cur.fetchone()
        self._cur.fetchall()
        return row

    def sql_query_value(self, sql, parms=()):
        return self.sql_query_row(sql, parms)[0]

    # Utilities =====

    def have_db(self):
        if self._db is None:
            return False
        else:
            return True

    def have_cursor(self):
        if self._cur is None:
            return False
        else:
            return True

    def lastrowid(self):
        return self._cur.lastrowid

    def disconnect(self):
        if self.have_cursor():
            self._cur.close()
        if self.have_db():
            self._db.close()
        self._cur = None
        self._db = None

    def begin_transaction(self):
        if self.have_db():
            if self._database == 'sqlite':
                self.sql_do("BEGIN TRANSACTION")
            elif self._database == 'mysql':
                self.sql_do("START TRANSACTION")

    def rollback(self):
        if self.have_db():
            self._db.rollback()

    def commit(self):
        if self.have_db():
            self._db.commit()

    # destructor
    def __del__(self):
        self.disconnect()


MY_HOST = 'pluto.local'
MY_USER = 'appuser'
MY_PASS = 'Spartacus'


def main():
    try:
        db = BWDB(dbms='sqlite', database='../db/scratch.db')
        # db = BWDB(dbms='mysql', host=MY_HOST, user=MY_USER, password=MY_PASS,
        #           database='scratch')

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

        # create table
        db.sql_do(create_table)
        db.table = "temp"

        print("populate table")
        insert_rows = (
            ("Jimi Hendrix", "Guitar"),
            ("Miles Davis", "Trumpet"),
            ("Billy Cobham", "Drums"),
            ("Charlie Bird", "Saxophone"),
            ("Oscar Peterson", "Piano"),
            ("Marcus Miller", "Bass"),
        )

        print(f"not adding {len(insert_rows)} rows (rollback)")
        db.begin_transaction()
        db.sql_do_many_nocommit("INSERT INTO temp (name, description) VALUES (?,?)", insert_rows)
        print("rollback")
        db.rollback()

        print(f"adding {len(insert_rows)} rows")
        db.begin_transaction()
        numrows = db.sql_do_many_nocommit("INSERT INTO temp (name, description) VALUES (?,?)", insert_rows)
        db.commit()
        print(f"added {numrows} rows")

        print(f"there are {db.sql_query_value('SELECT COUNT(*) FROM temp')} rows")

        for row in db.sql_query("SELECT * FROM temp"):
            print(row)

        print()
        print("find more than one row (%s%)")
        for row in db.sql_query("SELECT * FROM temp WHERE name LIKE ?", ("%s%",)):
            print(row)

        print()
        print("search for %Bird%")
        row = db.sql_query_row("SELECT * FROM temp WHERE name LIKE ?", ("%Bird%",))
        row_id = None
        if row is not None:
            row_id = row[0]
            print(f"found row {row_id}")
            print(row)

        print()
        print(f"update row {row_id}")
        numrows = db.sql_do("UPDATE temp SET name = 'The Bird' WHERE id = ?", (row_id,))
        print(f"{numrows} row(s) modified")
        print(db.sql_query_row("SELECT * FROM temp WHERE id = ?", (row_id,)))

        print()
        print("add a row")
        db.sql_do("INSERT INTO temp (name, description) VALUES (?,?)", ("Bob Dylan", "Harmonica"))
        row_id = db.lastrowid()
        print(f"row {row_id} added")
        print(db.sql_query_row("SELECT * FROM temp WHERE id = ?", (row_id,)))

        print()
        print("delete a row (Cobham)")
        numrows = db.sql_do("DELETE FROM temp WHERE name LIKE ?", ("%Cobham%",))
        print(f"{numrows} row(s) deleted")

        print()
        print("print remaining rows")
        for row in db.sql_query("SELECT * FROM temp"):
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
