import psycopg

class PostgresExecutor:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.conn = None

    def connect(self):
        if self.conn is None or self.conn.closed:
            self.conn = psycopg.connect(self.connection_string)

    def execute_query(self, query, params=None):
        self.connect()
        with self.conn.cursor() as cur:
            cur.execute(query, params)
            if cur.description:  # It's a SELECT query
                return cur.fetchall()
            else:
                self.conn.commit()
                return cur.rowcount  # Number of rows affected

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
