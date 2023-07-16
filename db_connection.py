from dotenv import load_dotenv
import psycopg2
import os

load_dotenv()


class DBConnection:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )
        self.cursor = self.conn.cursor()

    def execute(self, query):
        self.cursor.execute(query)
        self.conn.commit()

    def fetch(self, query):
        self.cursor.execute(query)
        column_names = [desc[0] for desc in self.cursor.description]
        rows = self.cursor.fetchall()
        return rows

    def __del__(self):
        self.cursor.close()
        self.conn.close()
