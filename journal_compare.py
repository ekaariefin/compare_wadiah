from utils import load_and_prepare_data, generate_html_report, compare_data_utils
from datetime import datetime
import webbrowser
import logging
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def extract_nomor_seri(nomor_referensi):
    """Ambil 23 karakter pertama dari nomor referensi"""
    if isinstance(nomor_referensi, str):
        return nomor_referensi[:23]


def analyzed(batch_id, start_date, end_date):
    print(f"\U0001F504 Membaca file untuk batch ID: {batch_id}...")

    file1 = f"oracle_{batch_id}.csv"
    file2 = f"pg_{batch_id}.csv"

    print(f"\U0001F50D Membaca file {file1} dan {file2}")
    df1, df2 = load_and_prepare_data(file1, file2, delimiter1=';', delimiter2=';')

    print("\U0001F50D Membandingkan data berdasarkan NOMOR_SERI...")
    cocok, tidak_cocok = compare_data_utils(df1, df2)

    print(f"\u2705 Data cocok     : {len(cocok)}")
    print(f"\u274C Data tidak cocok : {len(tidak_cocok)}")

    filename = f"wadiah_compare_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
    generate_html_report(df1, df2, cocok, tidak_cocok, filename, start_date, end_date)

    print(f"\U0001F4C1 Laporan berhasil dibuat: {filename}")
    webbrowser.open(f"file://{os.path.abspath(filename)}")
