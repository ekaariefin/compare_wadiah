import pandas as pd
import logging
import uuid
import os
from datetime import datetime
from journal_pg import fetch_xip_data
from journal_ibank import get_oracle_data
from journal_compare import analyzed

start_date = '2024-08-01'
end_date = '2024-09-01'

BATCH_ID = str(uuid.uuid4())

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def main():
    os.makedirs("data", exist_ok=True)
    # Step 1: Ambil data dari Oracle
    get_oracle_data(start_date, end_date, BATCH_ID)

    # Step 2: Ambil data dari PostgreSQL
    data = fetch_xip_data(start_date, end_date)
    if data:
        df = pd.DataFrame(data)

        # Mapping kolom referensi
        if 'nomor_referensi' in df.columns:
            df['REFERENCE_NO'] = df['nomor_referensi']
        else:
            logging.warning("Kolom 'nomor_referensi' tidak ditemukan pada data PostgreSQL.")

        if 'nomor_referensi_fwd' in df.columns:
            df['REFERENCE_NO_FWD'] = df['nomor_referensi_fwd']
        else:
            logging.warning("Kolom 'nomor_referensi_fwd' tidak ditemukan pada data PostgreSQL.")

        filename = os.path.join("data", f"pg_{BATCH_ID}.csv")
        df.to_csv(filename, index=False, sep=';', encoding='utf-8-sig')
        logging.info(f"Data PostgreSQL berhasil diekspor ke file: {filename}")

        # Step 3: Proses Compare
        logging.info("Memulai proses perbandingan data...")
        analyzed(BATCH_ID, start_date, end_date)
    else:
        logging.warning("Tidak ada data PostgreSQL untuk diekspor.")

if __name__ == "__main__":
    main()
