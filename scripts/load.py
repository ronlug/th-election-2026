import psycopg2
from extract import get_comments_mockup
import json


def connect_to_db():
    print("Connecting to PostgreSQL database...")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="warehouse",
            user="admin",
            password="P@ssw0rd"
        )
        print(conn)
        return conn
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    print("Creating table if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_comment_data (
                    id SERIAL PRIMARY KEY,
                    raw_data JSONB,
                    dw_insert_date TIMESTAMP DEFAULT NOW()
                );
        """)
        conn.commit()
        print("Table was created.")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        

def insert_records(conn, data):
    try:
        cursor = conn.cursor()
        query = "INSERT INTO dev.raw_comment_data (raw_data) VALUES (%s)"
        cursor.execute(query, (json.dumps(data),))
        conn.commit()
        print("Data successfully inserted.")
    except psycopg2.Error as e:
        print(f"Error inserting data into the database {e}")
        raise

    
def main():
    try:
        conn = connect_to_db()
        create_table(conn)
        data = get_comments_mockup()
        insert_records(conn, data)
    except Exception as e:
        print(f"An error occured during execution: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    main()