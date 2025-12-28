import streamlit as st
from pathlib import Path
import pandas as pd
import io

from utils.db import read_table
from utils.auth import authenticate, register_user
from utils.ingest_excel import ingest_hotel_kinerja, ingest_absensi
from utils.ingest_excel import normalize_columns

# ======================
# HELPER (ANTI 2,025)
# ======================
def show_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "tahun" in df.columns:
        df["tahun"] = df["tahun"].astype(str)
    return df

# ======================
# KONFIGURASI HALAMAN
# ======================
st.set_page_config(
    page_title="Dashboard VHT-S",
    layout="wide"
)

# ======================
# SESSION STATE LOGIN
# ======================
if "page" not in st.session_state:
    st.session_state.page = "login"
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "role" not in st.session_state:
    st.session_state.role = None

# ======================
# LOGIN / REGISTER
# ======================
if not st.session_state.logged_in:

    if st.session_state.page == "login":
        st.title("🔐 Login Dashboard VHT-S")

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Login"):
                role = authenticate(username, password)
                if role:
                    st.session_state.logged_in = True
                    st.session_state.role = role
                    st.rerun()
                else:
                    st.error("Username atau password salah")

        with col2:
            if st.button("Buat Akun"):
                st.session_state.page = "register"
                st.rerun()

        st.stop()

    if st.session_state.page == "register":
        st.title("📝 Registrasi Akun")

        new_user = st.text_input("Username baru")
        new_pass = st.text_input("Password", type="password")
        role = st.selectbox("Role", ["viewer", "admin"])

        if st.button("Daftar"):
            register_user(new_user, new_pass, role)
            st.success("Akun berhasil dibuat")
            st.session_state.page = "login"
            st.rerun()

        if st.button("⬅️ Kembali"):
            st.session_state.page = "login"
            st.rerun()

        st.stop()

# ======================
# HEADER
# ======================
st.title("📊 Dashboard VHT-S")
st.sidebar.success(f"👤 Login sebagai: {st.session_state.role}")

if st.sidebar.button("Logout"):
    st.session_state.logged_in = False
    st.session_state.role = None
    st.rerun()

# ======================
# BULAN
# ======================
BULAN_MAP = {
    "Januari": 1, "Februari": 2, "Maret": 3, "April": 4,
    "Mei": 5, "Juni": 6, "Juli": 7, "Agustus": 8,
    "September": 9, "Oktober": 10, "November": 11, "Desember": 12
}
BULAN_REVERSE = {v: k for k, v in BULAN_MAP.items()}

# ======================
# LOAD DATA
# ======================
df_hotel = read_table("hotel_kinerja")
df_absen = read_table("absensi")

df_hotel["tahun"] = pd.to_numeric(df_hotel["tahun"], errors="coerce")
df_hotel["bulan"] = pd.to_numeric(df_hotel["bulan"], errors="coerce")

if not df_absen.empty:
    df_absen["tahun"] = pd.to_numeric(df_absen["tahun"], errors="coerce")

# ======================
# FILTER GLOBAL
# ======================
st.sidebar.header("🎛 Filter Data")

tahun_list = sorted(df_absen["tahun"].dropna().astype(int).unique())
tahun_pilih = st.sidebar.selectbox("Tahun", tahun_list)

df_tahun = df_absen[df_absen["tahun"] == tahun_pilih]
bulan_min = int(df_tahun["bulan"].min())
bulan_max = int(df_tahun["bulan"].max())

bulan_awal = BULAN_MAP[
    st.sidebar.selectbox(
        "Dari Bulan",
        BULAN_MAP.keys(),
        index=list(BULAN_MAP.values()).index(bulan_min)
    )
]
bulan_akhir = BULAN_MAP[
    st.sidebar.selectbox(
        "Sampai Bulan",
        BULAN_MAP.keys(),
        index=list(BULAN_MAP.values()).index(bulan_max)
    )
]

df_absen_f = df_absen[
    (df_absen["tahun"] == tahun_pilih) &
    (df_absen["bulan"].between(bulan_awal, bulan_akhir))
]

# ======================
# TABS
# ======================
tab1, tab2 = st.tabs(["🏨 Kinerja Hotel", "👥 Absensi"])

with tab1:
    st.subheader("🏨 Monitoring Kinerja Hotel")

    if df_hotel.empty:
        st.info("Data kinerja hotel belum tersedia.")
        st.stop()

    # ======================
    # PASTIKAN TIPE DATA BENAR
    # ======================
    df_hotel["tahun"] = pd.to_numeric(df_hotel["tahun"], errors="coerce")
    df_hotel["bulan"] = pd.to_numeric(df_hotel["bulan"], errors="coerce")

    # ======================
    # TEMPLATE BLOK INDIKATOR
    # ======================
    def indikator_section(nama, kolom):

        st.markdown(f"## 📊 {nama}")

        col1, col2, col3 = st.columns(3)

        with col1:
            tahun_list = sorted(df_hotel["tahun"].dropna().astype(int).unique())
            tahun_pilih = st.selectbox(
                "Tahun",
                tahun_list,
                key=f"{kolom}_tahun"
            )

        df_tahun = df_hotel[df_hotel["tahun"] == tahun_pilih]

        with col2:
            bulan_awal = st.selectbox(
                "Dari Bulan",
                BULAN_MAP.keys(),
                index=list(BULAN_MAP.values()).index(int(df_tahun["bulan"].min())),
                key=f"{kolom}_bulan_awal"
            )

        with col3:
            bulan_akhir = st.selectbox(
                "Sampai Bulan",
                BULAN_MAP.keys(),
                index=list(BULAN_MAP.values()).index(int(df_tahun["bulan"].max())),
                key=f"{kolom}_bulan_akhir"
            )

        df_f = df_hotel[
            (df_hotel["tahun"] == tahun_pilih) &
            (df_hotel["bulan"].between(
                BULAN_MAP[bulan_awal],
                BULAN_MAP[bulan_akhir]
            ))
        ]

        hotel_list = sorted(df_f["hotel"].dropna().unique())
        hotel_pilih = st.multiselect(
            "Pilih Hotel (kosongkan = semua)",
            hotel_list,
            key=f"{kolom}_hotel"
        )

        if hotel_pilih:
            df_f = df_f[df_f["hotel"].isin(hotel_pilih)]

        # ======================
        # GRAFIK
        # ======================
        if df_f.empty:
            st.info("Tidak ada data sesuai filter.")
            return pd.DataFrame()

        chart_df = (
            df_f.groupby("bulan")[kolom]
            .mean()
            .sort_index()
        )

        chart_df.index = chart_df.index.map(BULAN_REVERSE)

        if chart_df.shape[0] == 1:
            st.bar_chart(chart_df)
        else:
            st.line_chart(chart_df)

        # ======================
        # TABEL
        # ======================
        tabel = (
            df_f[["hotel", "bulan", kolom]]
            .sort_values(["hotel", "bulan"])
        )
        tabel["bulan"] = tabel["bulan"].map(BULAN_REVERSE)

        st.dataframe(tabel, use_container_width=True)

        return tabel

    # ======================
    # PANGGIL SEMUA INDIKATOR
    # ======================
    tpk_tbl   = indikator_section("TPK", "tpk")
    gpr_tbl   = indikator_section("GPR", "gpr")
    tptt_tbl  = indikator_section("TPTT", "tptt")
    rlmta_tbl = indikator_section("RLMTA", "rlmta")
    rlmtn_tbl = indikator_section("RLMTN", "rlmtn")

    # ======================
    # DOWNLOAD EXCEL
    # ======================
    st.markdown("## ⬇️ Download Kinerja Hotel (Excel)")

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        tpk_tbl.to_excel(writer, sheet_name="TPK", index=False)
        gpr_tbl.to_excel(writer, sheet_name="GPR", index=False)
        tptt_tbl.to_excel(writer, sheet_name="TPTT", index=False)
        rlmta_tbl.to_excel(writer, sheet_name="RLMTA", index=False)
        rlmtn_tbl.to_excel(writer, sheet_name="RLMTN", index=False)

    st.download_button(
        "📥 Download Excel Kinerja Hotel",
        data=output.getvalue(),
        file_name="kinerja_hotel.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ======================
# TAB ABSENSI (FINAL BENAR)
# ======================
with tab2:
    st.subheader("👥 Monitoring Absensi PML & PCL")

    role_filter = st.radio(
        "Tampilkan",
        ["Gabungan", "PML", "PCL"],
        horizontal=True
    )

    # ===== ambil daftar nama =====
    if role_filter == "PML":
        nama_list = sorted(df_absen_f["pml"].dropna().unique())
    elif role_filter == "PCL":
        nama_list = sorted(df_absen_f["pcl"].dropna().unique())
    else:
        nama_list = sorted(
            pd.concat([df_absen_f["pml"], df_absen_f["pcl"]])
            .dropna()
            .unique()
        )

    nama_pilih = st.multiselect(
        "Pilih Nama (kosongkan = semua)",
        nama_list
    )

    # ===== bangun data tampilan =====
    rows = []

    for _, r in df_absen_f.iterrows():

        if role_filter in ["Gabungan", "PML"]:
            if not nama_pilih or r["pml"] in nama_pilih:
                rows.append({
                    "Bulan": BULAN_REVERSE.get(int(r["bulan"]), r["bulan"]),
                    "Nama": r["pml"],
                    "Role": "PML",
                    "Target": r["target"],
                    "Realisasi": r["realisasi"],
                    "Persentase": r["persentase"]
                })

        if role_filter in ["Gabungan", "PCL"]:
            if not nama_pilih or r["pcl"] in nama_pilih:
                rows.append({
                    "Bulan": BULAN_REVERSE.get(int(r["bulan"]), r["bulan"]),
                    "Nama": r["pcl"],
                    "Role": "PCL",
                    "Target": r["target"],
                    "Realisasi": r["realisasi"],
                    "Persentase": r["persentase"]
                })

    df_view = pd.DataFrame(rows)

    # ======================
    # GRAFIK (BERUBAH SESUAI NAMA)
    # ======================
    st.markdown("### 📊 Grafik Absensi per Nama")

    if not df_view.empty:
        chart_df = (
            df_view
            .pivot_table(
                index="Bulan",
                columns="Nama",
                values="Persentase",
                aggfunc="mean"
            )
            .sort_index()
        )

        if chart_df.shape[0] == 1:
            st.bar_chart(chart_df)
        else:
            st.line_chart(chart_df)
    else:
        st.info("Tidak ada data sesuai filter.")

    # ======================
    # TABEL
    # ======================
    st.markdown("### 📋 Tabel Absensi")
    st.dataframe(
        df_view.sort_values(["Bulan", "Role", "Nama"]),
        use_container_width=True
    )

    # ======================
    # DOWNLOAD (EXCEL SAJA - FIX)
    # ======================
    st.markdown("### ⬇️ Download Data (Excel)")

    import io
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_view.to_excel(writer, index=False, sheet_name="Absensi")

    st.download_button(
        label="📥 Download Excel",
        data=output.getvalue(),
        file_name="absensi.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# ======================
# ADMIN PANEL
# ======================
if st.session_state.role == "admin":
    st.sidebar.divider()
    st.sidebar.header("📤 Admin Panel")

    UPLOAD_DIR = Path("data/uploads")
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

    jenis_data = st.sidebar.selectbox("Jenis Data", ["Kinerja Hotel", "Absensi"])
    tahun_input = st.sidebar.selectbox("Tahun Data", list(range(2020, 2031)))
    bulan_input_nama = st.sidebar.selectbox("Bulan Data", BULAN_MAP.keys())
    bulan_input = BULAN_MAP[bulan_input_nama]

    uploaded_file = st.sidebar.file_uploader("Upload Excel", type=["xlsx"])

    if uploaded_file:
        save_path = UPLOAD_DIR / uploaded_file.name
        with open(save_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        df_preview = normalize_columns(pd.read_excel(save_path))
        st.dataframe(df_preview, use_container_width=True)

        if st.button("🚀 INGEST KE DATABASE"):
            if jenis_data == "Kinerja Hotel":
                ingest_hotel_kinerja(save_path, tahun_input, bulan_input)
            else:
                ingest_absensi(save_path, tahun_input, bulan_input)

            st.success("✅ Data berhasil di-ingest")
            st.rerun()
