import psycopg2, os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
DBNAME = os.getenv("DBNAME")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = os.getenv("PORT")


class PostgresDatabase:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=HOST, dbname=DBNAME, user=USER, password=PASSWORD, port=PORT
        )
        self.cur = self.conn.cursor()

    def runQuery(self, query: str):
        self.cur.execute(query)
        self.conn.commit()

    def fetchOne(self):
        return self.cur.fetchone()

    def fetchAll(self):
        return self.cur.fetchall()

    def __del__(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()
