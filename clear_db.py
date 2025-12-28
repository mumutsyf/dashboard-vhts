import sqlite3

conn = sqlite3.connect("db/vhts.db")
cur = conn.cursor()

cur.execute("DELETE FROM hotel_kinerja;")
cur.execute("DELETE FROM absensi;")

conn.commit()
conn.close()

print("✅ Data hotel_kinerja & absensi berhasil dihapus")
