import sqlite3


class DBConnection:
    instance = None
    # cursor = None
    # conn = None
    # name = None

    def __new__(cls, *args, **kwargs):
        if cls.instance is None:
            cls.instance = super().__new__(DBConnection)
            return cls.instance
        return cls.instance

    def __init__(self, db_name='you-db-name'):
        self.name = db_name
        # connect takes url, dbname, user-id, password
        self.conn = self.connect(db_name)
        self.cursor = self.conn.cursor()

    def connect(self):
        try:
            return sqlite3.connect(self.name)
        except sqlite3.Error as e:
            pass

    def run_query(self, query):
        return self.cursor.execute(query)

    def __del__(self):
        self.cursor.close()
        self.conn.close()
