import sqlite3
import pandas as pd
from pathlib import Path

DB_DIR = Path("db")
DB_PATH = DB_DIR / "vhts.db"


def init_db():
    """
    Inisialisasi database VHT-S
    """
    DB_DIR.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # =========================
    # TABEL ABSENSI
    # =========================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS absensi (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tanggal DATE,
        tahun INTEGER,
        bulan INTEGER,
        pml TEXT,
        pcl TEXT,
        target INTEGER,
        realisasi INTEGER,
        persentase REAL
    )
    """)

    # =========================
    # TABEL KINERJA HOTEL
    # =========================
    cur.execute("""
    CREATE TABLE IF NOT EXISTS hotel_kinerja (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tanggal DATE,
        tahun INTEGER,
        bulan INTEGER,
        hotel TEXT,
        pml TEXT,
        pcl TEXT,
        tpk REAL,
        gpr REAL,
        tptt REAL,
        rlmta REAL,
        rlmtn REAL
    )
    """)

    conn.commit()
    conn.close()


def get_connection():
    return sqlite3.connect(DB_PATH)


def read_table(table_name: str) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df
