import pandas as pd
import logging
import uuid
from datetime import datetime
from journal_pg import fetch_xip_data
from journal_ibank import get_oracle_data
from journal_compare import analyzed

start_date = '2024-11-01'
end_date = '2024-12-01'

# BATCH_ID = str(uuid.uuid4())
BATCH_ID = 'e27e2fc9-46af-4f2a-a9d3-899e493df5d9'

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def main():
    # analyzed(BATCH_ID, start_date, end_date)

    # Step 1: Ambil data dari Oracle
    get_oracle_data(start_date, end_date, BATCH_ID)

    # Step 2: Ambil data dari PostgreSQL
    data = fetch_xip_data(start_date, end_date)
    if data:
        df = pd.DataFrame(data)

        # Tambahkan kolom NOMOR_SERI (potongan 23 karakter dari nomor_referensi)
        if 'nomor_referensi' in df.columns:
            df['NOMOR_SERI'] = df['nomor_referensi'].astype(str).str[:23]
        else:
            logging.warning("Kolom 'nomor_referensi' tidak ditemukan pada data PostgreSQL.")

        filename = f"pg_{BATCH_ID}.csv"
        df.to_csv(filename, index=False, sep=';', encoding='utf-8-sig')
        logging.info(f"Data PostgreSQL berhasil diekspor ke file: {filename}")

        # Step 3: Proses Compare
        logging.info("Memulai proses perbandingan data...")
        analyzed(BATCH_ID, start_date, end_date)
    else:
        logging.warning("Tidak ada data PostgreSQL untuk diekspor.")

if __name__ == "__main__":
    main()
