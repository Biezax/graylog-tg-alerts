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
            event_id TEXT PRIMARY KEY,
            event_title TEXT,
            start_date REAL,
            end_date REAL,
            last_timestamp REAL,
            event_started INTEGER DEFAULT 0,
            event_ended INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def get_db():
    return sqlite3.connect(DATABASE_PATH)
