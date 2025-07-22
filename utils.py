import os
import datetime
import pandas as pd
import logging
import smtplib
from email.message import EmailMessage
from config import SMTP_CONFIG

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

report_recipient = [
    'eka_prasetyo@bcasyariah.co.id'
]


def load_and_prepare_data(file_oracle, file_pg, delimiter1=';', delimiter2=';'):
    """Membaca dua file CSV dan menormalisasi kolomnya."""
    logging.info(f"Membaca file Oracle: {file_oracle}")
    df_oracle = pd.read_csv(file_oracle, delimiter=delimiter1, dtype=str)

    logging.info(f"Membaca file PG: {file_pg}")
    df_pg = pd.read_csv(file_pg, delimiter=delimiter2, dtype=str)

    df_oracle.columns = df_oracle.columns.str.strip().str.upper()
    df_pg.columns = df_pg.columns.str.strip().str.upper()

    logging.info("Data berhasil dimuat dan kolom dinormalisasi.")
    return df_oracle, df_pg


def parse_float(value):
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '.').strip()
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0


def compare_and_export_csv(df_pg, df_oracle, batch_id, output_dir='reports'):
    # Normalisasi kolom
    df_pg.columns = df_pg.columns.str.strip().str.upper()
    df_oracle.columns = df_oracle.columns.str.strip().str.upper()

    if 'NOMOR_REFERENSI' not in df_pg.columns or 'REFERENCE_NO' not in df_oracle.columns:
        raise KeyError("Kolom 'NOMOR_REFERENSI' di df_pg atau 'REFERENCE_NO' di df_oracle tidak ditemukan.")

    df_pg['NOMOR_REFERENSI'] = df_pg['NOMOR_REFERENSI'].astype(str).str.strip()
    df_oracle['REFERENCE_NO'] = df_oracle['REFERENCE_NO'].astype(str).str.strip()

    set_pg = set(df_pg['NOMOR_REFERENSI'])
    set_oracle = set(df_oracle['REFERENCE_NO'])

    # PG -> Oracle
    rows_pg_to_oracle = []
    for _, row in df_pg.iterrows():
        nomor_ref = row.get('NOMOR_REFERENSI', '').strip()
        status = 'Cocok' if nomor_ref in set_oracle else 'Tidak Cocok, Ada di Downstream namun tidak ada di GL'
        rows_pg_to_oracle.append({
            'Nomor_Referensi': nomor_ref,
            'Nomor_Referensi_FWD': row.get('NOMOR_REFERENSI_FWD', ''),
            'Branch_Code': str(row.get('BRANCH_CODE', '')).zfill(3),
            'Deskripsi': row.get('KETERANGAN', '-'),
            'Status Compare': status
        })

    df_pg_to_oracle = pd.DataFrame(rows_pg_to_oracle)
    df_pg_to_oracle = df_pg_to_oracle.drop_duplicates(subset='Nomor_Referensi')
    df_pg_to_oracle.insert(0, 'No', range(1, len(df_pg_to_oracle) + 1))

    # Oracle -> PG
    diff_oracle = set_oracle - set_pg
    rows_oracle_to_pg = []
    for _, row in df_oracle[df_oracle['REFERENCE_NO'].isin(diff_oracle)].iterrows():
        rows_oracle_to_pg.append({
            'Nomor_Referensi': row.get('REFERENCE_NO', ''),
            'Nomor_Referensi_FWD': row.get('REFERENCE_NO_FWD', ''),
            'Branch_Code': str(row.get('BRANCH_CODE', '')).zfill(3),
            'Deskripsi': row.get('DESCRIPTION', '-'),
            'Status Compare': 'Tidak Cocok, Ada di GL namun tidak ada di Downstream'
        })

    df_oracle_to_pg = pd.DataFrame(rows_oracle_to_pg)
    df_oracle_to_pg = df_oracle_to_pg.drop_duplicates(subset='Nomor_Referensi')
    df_oracle_to_pg.insert(0, 'No', range(1, len(df_oracle_to_pg) + 1))

    # Simpan ke file
    os.makedirs(output_dir, exist_ok=True)
    path_pg = os.path.join(output_dir, f'{batch_id}_downstream_to_gl.csv')
    path_oracle = os.path.join(output_dir, f'{batch_id}_gl_to_downstream.csv')

    df_pg_to_oracle.to_csv(path_pg, index=False)
    df_oracle_to_pg.to_csv(path_oracle, index=False)

    logging.info(f"File hasil compare disimpan:\n - {path_pg}\n - {path_oracle}")

    # Ambil hanya data yang tidak cocok
    df_pg_to_oracle_mismatch = df_pg_to_oracle[df_pg_to_oracle['Status Compare'] != 'Cocok']
    df_oracle_to_pg_mismatch = df_oracle_to_pg[df_oracle_to_pg['Status Compare'] != 'Cocok']

    # Gabungkan hasil mismatch
    summary_cols = ['Nomor_Referensi', 'Status Compare']
    mismatch_summary = pd.concat([
        df_pg_to_oracle_mismatch[summary_cols],
        df_oracle_to_pg_mismatch[summary_cols]
    ], ignore_index=True)

    # Bersihkan spasi
    mismatch_summary['Nomor_Referensi'] = mismatch_summary['Nomor_Referensi'].astype(str).str.strip()

    # Drop duplikat berdasarkan Nomor Referensi
    mismatch_summary = mismatch_summary.drop_duplicates(subset='Nomor_Referensi')

    return df_oracle_to_pg, df_pg_to_oracle, path_pg, path_oracle, mismatch_summary

import os
import smtplib
import logging
import datetime
from email.message import EmailMessage
from email.utils import make_msgid
from config import SMTP_CONFIG   # pastikan ini diimpor

report_recipient = [
    'eka_prasetyo@bcasyariah.co.id'
]

def send_email_with_attachments(start_date, end_date, batch_id, attachments, summary_text=None):
    def format_date(date_str):
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    formatted_start = format_date(start_date)
    formatted_end = format_date(end_date)

    msg = EmailMessage()
    msg['Subject'] = 'Laporan Compare Transaksi Tahapan Wadiah iB'
    msg['From'] = SMTP_CONFIG['mail_sender']
    msg['To'] = ', '.join(report_recipient)

    # Isi HTML
    html_body = f"""
    <html>
        <body style="font-family: Segoe UI, sans-serif; font-size: 14px; color: #333;">
            <p>Assalamu'alaikum Warahmatullahi Wabarakatuh,</p>

            <p>Berikut kami sampaikan laporan hasil perbandingan transaksi produk <strong>Tahapan Wadiah iB</strong>:</p>

            <ul>
                <li><strong>📅 Periode:</strong> {formatted_start} s.d. {formatted_end}</li>
                <li><strong>🆔 Batch ID:</strong> {batch_id}</li>
            </ul>

            <p>Terlampir file CSV hasil perbandingan data berikut:</p>
            <ul>
                <li>Downstream → GL</li>
                <li>GL → Downstream</li>
            </ul>
            <i>Data CSV telah disesuaikan dengan hasil compare dan telah dibersihkan dari duplikasi.</i>
            <p>Silahkan periksa lampiran untuk detail lebih lanjut.</p>
    """

    if summary_text:
        html_body += f"""
            <p><strong>Ringkasan hasil perbandingan (Tidak Cocok):</strong></p>
            {summary_text.strip()}
        """

    html_body += """
            <p>Terima kasih atas perhatian dan kerja samanya.</p>

            <p>Wassalamu'alaikum Warahmatullahi Wabarakatuh.</p>

            <p>Regards,<br>
            Tim IT Core Department</p>
        </body>
    </html>
    """

    msg.add_alternative(html_body, subtype='html')

    # Lampiran
    for file_path in attachments:
        with open(file_path, 'rb') as f:
            file_data = f.read()
            file_name = os.path.basename(file_path)
            msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

    logging.info("Mengirim email ke: %s", msg['To'])
    with smtplib.SMTP(SMTP_CONFIG['mail_host'], SMTP_CONFIG['mail_port']) as server:
        server.starttls()
        server.login(SMTP_CONFIG['mail_sender'], SMTP_CONFIG['mail_password'])
        server.send_message(msg)

    logging.info("✅ Email beserta lampiran berhasil dikirim.")
