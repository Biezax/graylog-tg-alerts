import sqlite3
import os

DATABASE_PATH = "data/alerts.db"

def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    
    c.execute('DROP TABLE IF EXISTS alerts')
    
    c.execute('''
        CREATE TABLE alerts (
            event_id TEXT,
            start_date REAL,
            event_title TEXT,
            end_date REAL,
            last_timestamp REAL,
            event_started INTEGER DEFAULT 0,
            event_ended INTEGER DEFAULT 0,
            PRIMARY KEY (event_id, start_date)
        )
    ''')
    conn.commit()
    conn.close()

def get_db():
    return sqlite3.connect(DATABASE_PATH)
