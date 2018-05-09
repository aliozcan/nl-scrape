import os
import psycopg2

_connection = f"host=postgres user={os.getenv('POSTGRES_USER')} password={os.getenv('POSTGRES_PASSWORD')}"
conn = psycopg2.connect(_connection)