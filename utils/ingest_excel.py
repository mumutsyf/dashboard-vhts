import sqlite3
import pandas as pd
from pathlib import Path
from datetime import datetime

# ======================================================
# CONFIG
# ======================================================
DB_PATH = Path("db/vhts.db")


# ======================================================
# BASIC UTILITIES
# ======================================================
def connect_db():
    return sqlite3.connect(DB_PATH)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
    )
    return df


def resolve_columns(df: pd.DataFrame, column_map: dict) -> dict:
    resolved = {}
    for std_col, aliases in column_map.items():
        for col in df.columns:
            if col in aliases:
                resolved[std_col] = col
                break

    missing = set(column_map.keys()) - set(resolved.keys())
    if missing:
        raise ValueError(
            f"❌ Kolom wajib tidak ditemukan: {missing}\n"
            f"📌 Kolom tersedia di file: {list(df.columns)}"
        )
    return resolved


def check_duplicate(conn, table: str, tahun: int, bulan: int):
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {table} WHERE tahun=? AND bulan=?",
        (tahun, bulan)
    )
    return cur.fetchone()[0] > 0


# ======================================================
# 🔥 HELPER PALING PENTING (FINAL)
# ======================================================
def clean_number(val):
    if pd.isna(val):
        return None
    return float(str(val).replace(",", "").strip())


# ======================================================
# COLUMN MAP
# ======================================================
HOTEL_COLUMN_MAP = {
    "hotel": ["hotel", "nama_hotel", "hotel_name"],
    "pml": ["pml"],
    "pcl": ["pcl"],
    "tpk": ["tpk", "tpk_persen"],
    "gpr": ["gpr"],
    "tptt": ["tptt"],
    "rlmta": ["rlmta"],
    "rlmtn": ["rlmtn"],
}

ABSENSI_COLUMN_MAP = {
    "pml": ["pml"],
    "pcl": ["pcl"],
    "target": ["target"],
    "realisasi": ["realisasi"],
}


# ======================================================
# INGEST HOTEL
# ======================================================
def ingest_hotel_kinerja(
    file_path: Path,
    tahun: int,
    bulan: int,
    allow_replace: bool = False
):
    # 🔥 PAKSA WAKTU DARI PARAMETER (ANTI 2,025)
    tahun = int(str(tahun).replace(",", ""))
    bulan = int(bulan)

    df = pd.read_excel(file_path)
    df = normalize_columns(df)

    # ======================================================
    # ATUR TAHUN & BULAN (PRIORITAS EXCEL)
    # ======================================================
    if "tahun" not in df.columns:
        df["tahun"] = tahun
    else:
        df["tahun"] = df["tahun"].astype(str).str.replace(",", "").astype(int)

    if "bulan" not in df.columns:
        df["bulan"] = bulan
    else:
        df["bulan"] = df["bulan"].astype(str).str.replace(",", "").astype(int)


    col = resolve_columns(df, HOTEL_COLUMN_MAP)

    conn = connect_db()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO hotel_kinerja (
                tanggal, tahun, bulan,
                hotel, pml, pcl,
                tpk, gpr, tptt, rlmta, rlmtn
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().date(),
            row["tahun"],
            row["bulan"],
            row[col["hotel"]],
            row[col["pml"]],
            row[col["pcl"]],
            clean_number(row[col["tpk"]]),
            clean_number(row[col["gpr"]]),
            clean_number(row[col["tptt"]]),
            clean_number(row[col["rlmta"]]),
            clean_number(row[col["rlmtn"]]),
        ))

    conn.commit()
    conn.close()


# ======================================================
# INGEST ABSENSI
# ======================================================
def ingest_absensi(
    file_path: Path,
    tahun: int,
    bulan: int,
    allow_replace: bool = False
):
    tahun = int(str(tahun).replace(",", ""))
    bulan = int(bulan)

    df = pd.read_excel(file_path)
    df = normalize_columns(df)

    # ======================================================
    # ATUR TAHUN & BULAN (PRIORITAS EXCEL)
    # ======================================================
    if "tahun" not in df.columns:
        df["tahun"] = tahun
    else:
        df["tahun"] = df["tahun"].astype(str).str.replace(",", "").astype(int)

    if "bulan" not in df.columns:
        df["bulan"] = bulan
    else:
        df["bulan"] = df["bulan"].astype(str).str.replace(",", "").astype(int)


    col = resolve_columns(df, ABSENSI_COLUMN_MAP)

    conn = connect_db()
    cur = conn.cursor()

    for _, row in df.iterrows():
        target = clean_number(row[col["target"]])
        realisasi = clean_number(row[col["realisasi"]])

        persentase = (realisasi / target * 100) if target not in (None, 0) else 0

        cur.execute("""
            INSERT INTO absensi (
                tanggal, tahun, bulan,
                pml, pcl,
                target, realisasi, persentase
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().date(),
            row["tahun"],
            row["bulan"],
            row[col["pml"]],
            row[col["pcl"]],
            target,
            realisasi,
            persentase
        ))

    conn.commit()
    conn.close()
