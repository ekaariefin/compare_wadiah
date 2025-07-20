import pandas as pd
import logging
import uuid
from datetime import datetime
from journal_pg import fetch_xip_data
from journal_ibank import get_oracle_data
from journal_compare import analyzed
# File: main.py
start_date = '2023-01-01' 
end_date = '2025-01-01'

# Batch ID
# batch_id = uuid.uuid4()
batch_id = "46126c6c-fb6a-45ef-8f26-0cefafef151c"

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')


if __name__ == "__main__":
    # fetch data from PostgreSQL and Oracle
    analyzed(str(batch_id))
    # get_oracle_data(start_date, end_date, str(batch_id))
    # data = fetch_xip_data(start_date, end_date)
    # if data:
    #     df = pd.DataFrame(data)

    #     # Buat nama file CSV dengan tanggal dan waktu
    #     filename = f"pg_{batch_id}.csv"

    #     # Ekspor ke CSV
    #     df.to_csv(filename, index=False, encoding='utf-8-sig')
    #     logging.info(f"Data berhasil diekspor ke file: {filename}")

    #     # Memulai Proses Compare
    #     logging.info(f"Memulai Proses Compare...")

    # else:
    #     logging.warning("Tidak ada data untuk diekspor.")