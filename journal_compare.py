from utils import load_and_prepare_data, compare_data, generate_html_report
from datetime import datetime
import webbrowser
import os

def analyzed(batch_id):
    print(f"\U0001F504 Membaca file {batch_id}dan menyiapkan data...")

    file1 = f"pg_{batch_id}.csv"
    file2 = f"oracle_{batch_id}.csv"
    print(f"\U0001F50D Membaca file {file1}")
    df1, df2 = load_and_prepare_data(file1, file2, delimiter1=',', delimiter2=';')

    print("\U0001F50D Membandingkan data (Sheet 2 terhadap Sheet 1)...")
    cocok, tidak_cocok = compare_data(df1, df2)

    print(f"\u2705 Data cocok: {len(cocok)}")
    print(f"\u274C Data tidak cocok: {len(tidak_cocok)}")

    filename = f"wadiah_compare_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
    generate_html_report(df1, df2, cocok, tidak_cocok, filename)

    print(f"\U0001F4C1 Laporan berhasil dibuat: {filename}")
    webbrowser.open(f"file://{os.path.abspath(filename)}")
