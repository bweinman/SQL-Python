#!/usr/bin/env python3
# Copyright 2021 BHG [bw.org]
# BWDB.py as of 2021-04-13 bw


# module version
__version__ = "3.1.11"

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
    """Simple Error class"""

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class BWDB:
    def __init__(self, **kwargs):
        self._db = None
        self._cur = None
        self._dbms = None
        self._database = None
        self._table = None
        self._column_names = None

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

        if 'table' in kwargs:
            self.table = kwargs['table']

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

    def set_table(self, table):
        self._table = self.sanitize_string(table)
        self.column_names()

    def get_table(self):
        return self._table

    # properties
    dbms = property(fget=get_dbms, fset=set_dbms)
    database = property(fget=get_database, fset=set_database)
    table = property(fget=get_table, fset=set_table)
    cursor = property(fget=get_cursor)

    # sql methods =====
    def sql_do_nocommit(self, sql, parms=()):
        """Execute an SQL statement"""
        self._cur.execute(sql, parms)
        return self._cur.rowcount

    def sql_do(self, sql, parms=()):
        """Execute an SQL statement"""
        self._cur.execute(sql, parms)
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

    # crud methods =====
    def column_names(self):
        """ Get column names """
        if self._column_names is not None:
            return self._column_names

        if self._dbms == 'sqlite':
            rows = self.sql_query(f"PRAGMA table_info ({self._table});")
            self._column_names = tuple(r[1] for r in rows)
        elif self._dbms == 'mysql':
            self._cur.execute(f"SELECT * FROM {self._table} LIMIT 1")
            self._cur.fetchall()
            self._column_names = self._cur.column_names
        else:
            raise BWErr("column_names: unknown _dbms")

        if self._column_names[0] != 'id':
            self._column_names = None
            raise BWErr("colum_names: no id column")
        elif len(self._column_names) < 2:
            self._column_names = None
            raise BWErr("colum_names: empty list")
        else:
            return self._column_names

    def count_rows(self):
        """ Returns number of rows in table """
        return self.sql_query_value(f"SELECT COUNT(*) FROM {self._table}")

    def get_row(self, row_id):
        """ Get rows from table – returns cursor """
        return self.sql_query_row(f"SELECT * FROM {self._table} WHERE id = ?", (row_id,))

    def get_rows(self):
        """ Get rows from table – returns cursor """
        return self.sql_query(f"SELECT * FROM {self._table}")

    def get_rows_limit(self, limit, offset=0):
        return self.sql_query(f"SELECT * FROM {self._table} LIMIT ? OFFSET ?",
                              (limit, offset))

    def add_row_nocommit(self, parms=()):
        colnames = self.column_names()
        numnames = len(colnames)
        if 'id' in colnames:
            numnames -= 1
        names_str = self.sql_colnames_string(colnames)
        values_str = self.sql_values_string(numnames)
        sql = f"INSERT INTO {self._table} ({names_str}) VALUES ({values_str})"
        return self.sql_do_nocommit(sql, parms)

    def add_row(self, parms=()):
        r = self.add_row_nocommit(parms)
        self.commit()
        return r

    def update_row_nocommit(self, row_id, dict_rec):
        """ Update row id with data in dict """
        if "id" in dict_rec.keys():  # don't update id column
            del dict_rec['id']

        keys = sorted(dict_rec.keys())  # get keys and values
        values = [dict_rec[v] for v in keys]
        update_string = self.sql_update_string(keys)
        sql = f"UPDATE {self._table} SET {update_string} WHERE id = ?"
        values.append(row_id)
        return self.sql_do_nocommit(sql, values)

    def update_row(self, row_id, dict_rec):
        r = self.update_row_nocommit(row_id, dict_rec)
        self.commit()
        return r

    def del_row_nocommit(self, row_id):
        return self.sql_do_nocommit(f"DELETE FROM {self._table} WHERE id = ?", (row_id,))

    def del_row(self, row_id):
        r = self.del_row_nocommit(row_id)
        self.commit()
        return r

    def find_row(self, colname, value):
        """ Find the first match and returns id or None """
        colname = self.sanitize_string(colname)  # sanitize params
        sql = f"SELECT * FROM {self._table} WHERE {colname} LIKE ?"
        row = self.sql_query_row(sql, (value,))
        if row:
            return row[0]
        else:
            return None

    def find_rows(self, colname, value):
        """ Find the first match and returns id or empty list """
        colname = self.sanitize_string(colname)  # sanitize params
        sql = f"SELECT * FROM {self._table} WHERE {colname} LIKE ?"
        row_ids = []
        for row in self.sql_query(sql, (value,)):
            row_ids.append(row[0])
        return row_ids

    # Utilities =====

    @staticmethod
    def version():
        return __version__

    @staticmethod
    def sanitize_string(s):
        """ Remove nefarious characters from a string """
        charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_-.% "
        san_string = ""
        for i in range(0, len(s)):
            if s[i] in charset:
                san_string += s[i]
            else:
                san_string += '_'
        return san_string

    @staticmethod
    def sql_colnames_string(colnames):
        names_str = ","
        if colnames[0] == "id":
            colnames = colnames[1:]
        return names_str.join(colnames)

    @staticmethod
    def sql_values_string(num):
        s = "?," * num
        return s[0:-1]

    @staticmethod
    def sql_update_string(colnames):
        update_string = ","
        for i in range(len(colnames)):
            colnames[i] += "=?"
        return update_string.join(colnames)

    def make_dict_row(self, row):
        return dict(zip(self.column_names(), row))

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

    def have_table(self, table_name=None):
        if table_name is None:
            table_name = self._table
        if table_name is None:
            return False
        if self._dbms == 'sqlite':
            rc = self.sql_query_value("SELECT COUNT(*) FROM sqlite_master WHERE type=? AND name=?",
                                      ('table', table_name))
            if rc > 0:
                return True
        if self._dbms == 'mysql':
            rc = self.sql_query_value("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
                                      (table_name,))
            if rc > 0:
                return True
        return False

    def lastrowid(self):
        return self._cur.lastrowid

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

    def disconnect(self):
        if self.have_cursor():
            self._cur.close()
        if self.have_db():
            self._db.close()
        self._cur = None
        self._db = None
        self._column_names = None

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

        print(f"BWDB version {db.version()}")
        print(f"dbms is {db.dbms}\n")

        # start clean
        db.sql_do("DROP TABLE IF EXISTS temp")

        print(f"have table {db.have_table('temp')}")

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
        print(f"have table {db.have_table()}")
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

        print("not add rows (rollback)")
        db.begin_transaction()
        for row in insert_rows:
            db.add_row_nocommit(row)
        db.rollback()

        print("add rows")
        db.begin_transaction()
        for row in insert_rows:
            db.add_row_nocommit(row)
        db.commit()
        print(f"added {len(insert_rows)} rows")

        print(f"there are {db.count_rows()} rows")

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
        numrows = db.update_row(row_id, {'name': 'The Bird', 'description': 'Tenor Sax'})
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
        db.begin_transaction()
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

        print()
        print("dict rows")
        db.dict_rows = True
        for row in db.get_rows_limit(5):
            print(db.make_dict_row(row))

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
