import oracledb
import pandas as pd

from tqdm import tqdm  # pip install tqdm
import csv
from datetime import datetime
from decimal import Decimal
from typing import List


oracledb.init_oracle_client(lib_dir=r"C:\instantclient_23_8")

config = {
    "host": "10.125.5.6",
    "port": 1521,
    "sid": "ibank",
    "user": "ibankcore",
    "password": "bcasharia711"
}

query = """
SELECT
    j.journal_date AS JournalDate,
    j.transaction_date,
    j.userid_create,
    j.journal_no,
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


def get_connection(cfg):
    dsn = f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={cfg['host']})(PORT={cfg['port']}))(CONNECT_DATA=(SID={cfg['sid']})))"
    return oracledb.connect(user=cfg['user'], password=cfg['password'], dsn=dsn)

def get_oracle_data(start_date: str, end_date: str, batch_id: str) -> None:
    output_file = f"oracle_{batch_id}.csv"
    conn = None
    try:
        print("🔌 Membuka koneksi ke Oracle (SID, Thick Mode)...")
        conn = get_connection(config)

        print("📥 Menjalankan query...")
        cursor = conn.cursor()
        cursor.execute(query, {'start_date': start_date, 'end_date': end_date})

        columns = [col[0] for col in cursor.description]

        print(f"💾 Mengekspor ke CSV dengan progress: {output_file}")
        with open(output_file, mode='w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(columns)

            batch_size = 1000
            total = 0

            with tqdm(desc="📤 Menulis data", unit="baris", ncols=80) as pbar:
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    writer.writerows(rows)
                    total += len(rows)
                    pbar.update(len(rows))

        print(f"✅ Selesai ekspor. Total baris: {total}")
    except Exception as e:
        print(f"❌ Terjadi error: {e}")
    finally:
        if conn is not None:
            conn.close()
            print("🔒 Koneksi ditutup.")
