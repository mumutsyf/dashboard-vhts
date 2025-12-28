import sqlite3
import hashlib
from pathlib import Path

DB_PATH = Path("db/vhts.db")


def connect_db():
    return sqlite3.connect(DB_PATH)


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


# =========================
# INIT TABLE USERS
# =========================
def init_auth_table():
    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


# =========================
# REGISTER USER
# =========================
def register_user(username: str, password: str, role="viewer"):
    init_auth_table()

    conn = connect_db()
    cur = conn.cursor()

    try:
        cur.execute("""
            INSERT INTO users (username, password_hash, role)
            VALUES (?, ?, ?)
        """, (username, hash_password(password), role))
        conn.commit()
    except sqlite3.IntegrityError:
        conn.close()
        raise ValueError("Username sudah terdaftar")

    conn.close()


# =========================
# AUTHENTICATE USER
# =========================
def authenticate(username: str, password: str):
    init_auth_table()

    conn = connect_db()
    cur = conn.cursor()

    cur.execute("""
        SELECT role, password_hash
        FROM users
        WHERE username = ?
    """, (username,))

    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    role, stored_hash = row
    if stored_hash == hash_password(password):
        return role

    return None
