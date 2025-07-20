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

# Jumphost dan RDS
JUMPHOST_IP = '10.31.1.221'
JUMPHOST_PORT = 22
JUMPHOST_USER = 'monitoring-app-dev'
JUMPHOST_PASSWORD = 'Syariah061820251012'

RDS_HOST = 'xip-aurora-db-124495977855.cluster-czpgxhhclhau.ap-southeast-3.rds.amazonaws.com'
RDS_PORT = 8097
RDS_DBNAME = 'dataprods-bcas'
RDS_1B_DBNAME = 'dbuat-1b-bcas'
RDS_USER = 'superman'
RDS_PASSWORD = 'Syariah@1'

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
            (JUMPHOST_IP, JUMPHOST_PORT),
            ssh_username=JUMPHOST_USER,
            ssh_password=JUMPHOST_PASSWORD,
            remote_bind_address=(RDS_HOST, RDS_PORT),
            local_bind_address=('127.0.0.1', 5433)
        ) as tunnel:
            logging.info("SSH Tunnel aktif.")

            conn = psycopg2.connect(
                host='127.0.0.1',
                port=5433,
                database=RDS_DBNAME,
                user=RDS_USER,
                password=RDS_PASSWORD
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
                    WHERE a.product_id = 'tahapan_wadiah_ib'
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
                    WHERE a.product_id = 'tahapan_wadiah_ib'
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
