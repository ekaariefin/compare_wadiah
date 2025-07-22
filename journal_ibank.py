import oracledb
import pandas as pd
import csv
import logging
import os
from tqdm import tqdm
from datetime import datetime
from decimal import Decimal
from typing import List
from config import ORACLE_CONFIG

# Inisialisasi klien Oracle
oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

# Konfigurasi logging
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/get_oracle_data.log"),
        logging.StreamHandler()
    ]
)

config = ORACLE_CONFIG

query = """
    SELECT
    j.journal_date AS JournalDate,
    j.transaction_date,
    j.userid_create,
    j.journal_no,
    j.branch_code,
    j.REFERENCE_NO,
    j.REFERENCE_NO_FWD,
    j.serial_no AS NomorSeri,
    j.description AS JurnalDescription,
    ji.amount_debit,
    ji.amount_credit,
    ji.description
FROM
    ibankcore.journal j
    INNER JOIN ibankcore.journalitem ji ON j.journal_no = ji.fl_journal
WHERE 
    j.journal_date >= TO_DATE(:start_date, 'YYYY-MM-DD')
    AND j.journal_date < TO_DATE(:end_date, 'YYYY-MM-DD')
    AND ji.fl_account = '2112003'
"""

def get_connection(config):
    dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={config['host']})(PORT={config['port']}))(CONNECT_DATA=(SID={config['sid']})))"
    return oracledb.connect(user=config['user'], password=config['password'], dsn=dsn)

def get_oracle_data(start_date: str, end_date: str, batch_id: str) -> None:
    os.makedirs("data", exist_ok=True)  # Membuat folder 'data' jika belum ada
    output_file = os.path.join("data", f"oracle_{batch_id}.csv")
    conn = None
    try:
        logging.info("Membuka koneksi ke Oracle...")
        conn = get_connection(config)

        logging.info("Menjalankan query...")
        cursor = conn.cursor()
        cursor.execute(query, {'start_date': start_date, 'end_date': end_date})

        columns = [col[0] for col in cursor.description]

        logging.info(f"Mengekspor hasil query ke file: {output_file}")
        with open(output_file, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(columns)

            batch_size = 1000
            total = 0

            with tqdm(desc="Menulis data", unit="baris", ncols=80) as pbar:
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    writer.writerows(rows)
                    total += len(rows)
                    pbar.update(len(rows))

        logging.info(f"Selesai ekspor. Total baris: {total}")
    except Exception as e:
        logging.error(f"Terjadi error: {e}", exc_info=True)
    finally:
        if conn is not None:
            conn.close()
            logging.info("Koneksi ke Oracle ditutup.")
