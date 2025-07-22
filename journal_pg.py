import logging
from sshtunnel import SSHTunnelForwarder
import psycopg2
import pyodbc
import uuid
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import hashlib
import hmac
import base64
import time
from datetime import datetime
from decimal import Decimal
from typing import List
import pandas as pd
from config import POSTGRES_CONFIG

pgcon = POSTGRES_CONFIG

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')


SESSID_CACHE = {
    "sessid": None,
    "timestamp": 0
}

def fetch_xip_data(start_date, end_date) -> List[dict]:
    result = []

    try:
        with SSHTunnelForwarder(
            (pgcon['ssh_host'], pgcon['ssh_port']),
            ssh_username=pgcon['ssh_username'],
            ssh_password=pgcon['ssh_password'],
            remote_bind_address=(pgcon['db_host'], pgcon['db_port']),
            local_bind_address=('127.0.0.1', 5433)
        ) as tunnel:
            logging.info("SSH Tunnel aktif.")

            conn = psycopg2.connect(
                host='127.0.0.1',
                port=5433,
                database=pgcon['db_name'],
                user=pgcon['db_user'],
                password=pgcon['db_password']
            )
            cur = conn.cursor()

            query = """
                SELECT * FROM (
                    SELECT
                        e.request_datetime,
                        e.response_datetime,
                        g.event_id,
                        g.account_id,
                        a.branch_code,
                        g.amount,
                        g.jenis_mutasi,
                        e.request_data::jsonb->>'nomor_referensi'      AS nomor_referensi,
                        e.request_data::jsonb->>'nomor_referensi_fwd'  AS nomor_referensi_fwd,
                        e.request_data::jsonb->>'keterangan'           AS keterangan,
                        CASE WHEN g.jenis_mutasi = 'D' THEN g.amount ELSE NULL END AS amount_debit,
                        CASE WHEN g.jenis_mutasi = 'C' THEN g.amount ELSE NULL END AS amount_credit
                    FROM account_created a
                    JOIN gl_posting g ON g.account_id = a.account_id
                    JOIN event_history_external e ON e.event_id::text = g.event_id::text
                    WHERE (a.product_id = 'tahapan_wadiah_ib' or a.product_id = 'rtjh')
                    AND e.request_datetime BETWEEN %s AND %s

                    UNION ALL

                    SELECT
                        e.request_datetime,
                        e.response_datetime,
                        g.event_id,
                        g.account_id,
                        a.branch_code,
                        g.amount,
                        g.jenis_mutasi,
                        e.request_data::jsonb->>'nomor_referensi'      AS nomor_referensi,
                        e.request_data::jsonb->>'nomor_referensi_fwd'  AS nomor_referensi_fwd,
                        e.request_data::jsonb->>'keterangan'           AS keterangan,
                        CASE WHEN g.jenis_mutasi = 'D' THEN g.amount ELSE NULL END AS amount_debit,
                        CASE WHEN g.jenis_mutasi = 'C' THEN g.amount ELSE NULL END AS amount_credit
                    FROM account_created a
                    JOIN gl_posting g ON g.account_id = a.account_id
                    JOIN event_history_externals e ON e.event_id::text = g.event_id::text
                    WHERE (a.product_id = 'tahapan_wadiah_ib' or a.product_id = 'rtjh')
                    AND e.request_datetime BETWEEN %s AND %s

                ) AS combined_result
                ORDER BY request_datetime DESC
            """
            cur.execute(query, (start_date, end_date, start_date, end_date))

            rows = cur.fetchall()

            # Aman: cek jika cur.description tersedia
            if cur.description:
                columns = [desc[0] for desc in cur.description]
                result = [dict(zip(columns, row)) for row in rows]
            else:
                logging.warning("cur.description kosong — kemungkinan query tidak mengembalikan hasil.")

            logging.info(f"Ambil data: {len(result)} baris")

            cur.close()
            conn.close()

    except Exception as e:
        logging.error(f"Fetch trib_list_account gagal: {e}", exc_info=True)

    return result
