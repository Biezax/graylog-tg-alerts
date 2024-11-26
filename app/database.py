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
            event_alert TEXT PRIMARY KEY,
            data TEXT,
            last_timestamp REAL,
            alert_sent INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()

def get_db():
    return sqlite3.connect(DATABASE_PATH)
