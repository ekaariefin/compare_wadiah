import datetime
import pandas as pd
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.message import EmailMessage
from config import SMTP_CONFIG
from tqdm import tqdm

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

report_recipient = [
    'eka_prasetyo@bcasyariah.co.id'
]

def parse_float(value):
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '.').strip()
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def load_and_prepare_data(file_oracle, file_pg, delimiter1=';', delimiter2=';'):
    df_oracle = pd.read_csv(file_oracle, delimiter=delimiter1, dtype=str)
    df_pg = pd.read_csv(file_pg, delimiter=delimiter2, dtype=str)

    # Strip semua kolom dari spasi untuk konsistensi
    df_oracle.columns = df_oracle.columns.str.strip()
    df_pg.columns = df_pg.columns.str.strip()

    # logging.info(f"Kolom di df_oracle: {df_oracle.columns.tolist()}")
    # logging.info(f"Top 1 data df_oracle:\n{df_oracle.head(1).to_dict(orient='records')}")

    if 'NOMORSERI' not in df_oracle.columns:
        raise KeyError("Kolom 'NOMORSERI' tidak ditemukan di file Oracle.")
    if 'nomor_referensi' not in df_pg.columns:
        raise KeyError("Kolom 'nomor_referensi' tidak ditemukan di file PG.")

    # PG: ambil 23 karakter pertama dari nomor referensi, simpan sebagai 'nomorseri'
    df_pg['nomorseri'] = df_pg['nomor_referensi'].str[:23]

    return df_oracle, df_pg

def compare_data_utils(df_oracle, df_pg):
    # Normalisasi nama kolom
    df_oracle.columns = df_oracle.columns.str.strip().str.upper()
    df_pg.columns = df_pg.columns.str.strip().str.upper()

    # logging.info(f"Header kolom file1_df (Oracle): {df_oracle.columns.tolist()}")
    # logging.info(f"Header kolom file2_df (Postgres): {df_pg.columns.tolist()}")

    # Validasi kolom
    if 'NOMORSERI' not in df_oracle.columns:
        raise KeyError("Kolom 'NOMORSERI' tidak ditemukan di file Oracle.")
    if 'NOMORSERI' not in df_pg.columns:
        raise KeyError("Kolom 'NOMORSERI' tidak ditemukan di file Postgres.")

    # Bersihkan dan setel kolom pembanding
    df_oracle['NOMORSERI'] = df_oracle['NOMORSERI'].astype(str).str.strip()
    df_pg['NOMORSERI'] = df_pg['NOMORSERI'].astype(str).str.strip()

    set_pg = set(df_pg['NOMORSERI'])

    cocok = []
    tidak_cocok = []

    for _, row in tqdm(df_oracle.iterrows(), total=len(df_oracle), desc="\U0001F4CA Membandingkan"):
        nomorseri = str(row.get('NOMORSERI', '')).strip()
        jurnaldesc = str(row.get('JURNALDESCRIPTION', '')).strip()
        amount_debit = row.get('AMOUNT_DEBIT', '')
        amount_credit = row.get('AMOUNT_CREDIT', '')

        status = 'Cocok' if nomorseri in set_pg else 'Tidak Cocok'

        row_dict = {
            'NOMORSERI': nomorseri,
            'JURNALDESCRIPTION': jurnaldesc,
            'AMOUNT_DEBIT': amount_debit,
            'AMOUNT_CREDIT': amount_credit,
            'STATUS COMPARE': status
        }

        if status == 'Cocok':
            cocok.append(row_dict)
        else:
            tidak_cocok.append(row_dict)

    return cocok, tidak_cocok

def calculate_debit_kredit(rows):
    """Menghitung total debit dan kredit dari list of dicts."""
    total_debit = sum(parse_float(row.get('AMOUNT_DEBIT')) for row in rows)
    total_kredit = sum(parse_float(row.get('AMOUNT_CREDIT')) for row in rows)
    return total_debit, total_kredit

def render_rows(data, status, display_columns, dummy_if_empty=False):
    """Membuat baris HTML untuk tabel."""
    if data:
        rows_html = ""
        for row in data:
            row_html = "<tr>"
            for col in display_columns:
                if col == 'STATUS COMPARE':
                    val = row.get(col, status)
                else:
                    val = row.get(col, '')
                # Pastikan val di-escape agar aman, tapi untuk sekarang cukup str(val)
                row_html += f"<td>{val}</td>"
            row_html += "</tr>"
            rows_html += row_html
        return rows_html
    elif dummy_if_empty:
        return f"<tr style='visibility:hidden;'>{''.join(f'<td>{col}</td>' for col in display_columns)}</tr>"
    else:
        return f"<tr><td colspan='{len(display_columns)}' class='text-center text-muted'>Tidak ada data</td></tr>"

def generate_html_table_section(title, data, display_columns, table_id, table_class, status_text='', dummy_if_empty=False):
    """Membuat section tabel HTML."""
    # logging.info(f"Top 1 data df_oracle:\n{data.head(1).to_dict(orient='records')}")
    
    rows_html = render_rows(data, status_text, display_columns, dummy_if_empty)
    thead_html = ''.join(f'<th>{col}</th>' for col in display_columns)

    return f"""
    <div class='section-header text-{table_class}'>
        <h5>{title}</h5>
    </div>
    <div class='data-section card p-3 shadow-sm'>
        <div class='table-responsive'>
            <table id='{table_id}' class='table table-striped table-bordered w-100 text-start'>
                <thead class='table-{table_class}'><tr>{thead_html}</tr></thead>
                <tbody>{rows_html}</tbody>
            </table>
        </div>
    </div>
    """

def generate_summary_rows(summary_data):
    rows = ""
    for desc, value, _, css_class in summary_data:
        # Auto tandai merah jika deskripsi mengandung 'Tidak Ditemukan'
        if "Tidak Ditemukan" in desc:
            css_class = "text-danger"
        class_attr = f'class="{css_class}"' if css_class else ""
        rows += f"<tr><td>{desc}</td><td {class_attr}>{value}</td></tr>\n"
    return rows


def send_summary_email(summary_data, recipient_email, start_date, end_date):
    def format_date(date_str):
        """Ubah format dari 'YYYY-MM-DD' ke 'DD-MM-YYYY'."""
        return datetime.datetime.strptime(date_str, "%Y-%m-%d").strftime("%d-%m-%Y")

    formatted_start = format_date(start_date)
    formatted_end = format_date(end_date)

    try:
        if isinstance(recipient_email, list):
            recipient_str = ', '.join(recipient_email)
        else:
            recipient_str = str(recipient_email)

        logging.info("Mengirim summary email ke: %s", recipient_str)

        msg = EmailMessage()
        msg['Subject'] = 'Laporan Compare Transaksi Tahapan Wadiah iB'
        msg['From'] = SMTP_CONFIG['mail_sender']
        msg['To'] = recipient_str

        summary_rows_html = generate_summary_rows(summary_data)

        html_content = f"""
            <html>
            <head>
                <style>
                    body {{
                        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                        font-size: 13.5px;
                        color: #212121;
                        line-height: 1.6;
                        margin: 0;
                        padding: 0;
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                        margin-top: 14px;
                    }}
                    th, td {{
                        border: 1px solid #dddddd;
                        text-align: left;
                        padding: 6px 10px;
                        vertical-align: middle;
                    }}
                    th {{
                        background-color: #f1f1f1;
                        font-weight: 600;
                        text-align: center;
                        vertical-align: middle;
                    }}
                    .text-danger {{
                        color: #c62828;
                        font-weight: bold;
                    }}
                    .text-success,
                    .text-info,
                    .text-warning {{
                        color: #212121;
                        font-weight: normal;
                    }}
                </style>
            </head>
            <body>
                <p>Assalamu'alaikum Warahmatullahi Wabarakatuh,</p>

                <p>Berikut kami sampaikan laporan summary perbandingan transaksi produk <strong>Tahapan Wadiah iB</strong>:</p>
                <p><strong>Periode:</strong> {formatted_start} s.d. {formatted_end}</p>

                <table>
                    <thead>
                        <tr>
                            <th>Keterangan</th>
                            <th>Hasil Compare</th>
                        </tr>
                    </thead>
                    <tbody>
                        {summary_rows_html}
                    </tbody>
                </table>

                <p>Terima kasih atas perhatian dan kerja samanya.</p>

                <p>Wassalamu'alaikum Warahmatullahi Wabarakatuh.</p>

                <br>
                <p>Regards,<br><strong>Tim IT Core Department</strong></p>
            </body>
            </html>
        """

        msg.set_content("Berikut adalah laporan summary perbandingan dalam format HTML.")
        msg.add_alternative(html_content, subtype='html')

        with smtplib.SMTP(SMTP_CONFIG['mail_host'], SMTP_CONFIG['mail_port']) as server:
            server.starttls()
            server.login(SMTP_CONFIG['mail_sender'], SMTP_CONFIG['mail_password'])
            server.send_message(msg)

        logging.info("✅ Summary email berhasil dikirim.")

    except Exception as e:
        logging.error("❌ Gagal mengirim email: %s", str(e))


def generate_html_report(df_pg, df_oracle, cocok, tidak_cocok, filename, start_date, end_date):
    """Membangun dan menyimpan laporan HTML."""
    total_1, total_2 = len(df_pg), len(df_oracle)
    total_found, total_not_found = len(cocok), len(tidak_cocok)

    display_columns = [
        'NOMORSERI', 'JURNALDESCRIPTION', 'AMOUNT_DEBIT', 'AMOUNT_CREDIT', 'STATUS COMPARE'
    ]

    total_debit_cocok, total_kredit_cocok = calculate_debit_kredit(cocok)
    total_debit_tidak, total_kredit_tidak = calculate_debit_kredit(tidak_cocok)

    summary_data = [
        ("Jumlah Data File 1", total_1, "primary", ""),
        ("Jumlah Data File 2", total_2, "primary", ""),
        ("Tidak Ditemukan", total_not_found, "danger", "text-danger"),
        ("Ditemukan", total_found, "success", "text-success"),
        ("Total Debit Ditemukan", f"Rp {total_debit_cocok:,.2f}", "info", "text-info"),
        ("Total Kredit Ditemukan", f"Rp {total_kredit_cocok:,.2f}", "info", "text-info"),
        ("Total Debit Tidak Ditemukan", f"Rp {total_debit_tidak:,.2f}", "warning", "text-warning"),
        ("Total Kredit Tidak Ditemukan", f"Rp {total_kredit_tidak:,.2f}", "warning", "text-warning")
    ]

    def render_summary_row(data):
        return ''.join([
            f"""
            <div class='card border-{color} summary-card'>
                <div class='card-body text-center'>
                    <h6 class='card-title'>{title}</h6>
                    <p class='card-text fw-bold {text_class}'>{count}</p>
                </div>
            </div>
            """
            for title, count, color, text_class in data
        ])

    summary_cards_html = f"""
        <div class='summary-row'>{render_summary_row(summary_data[:4])}</div>
        <div class='summary-row'>{render_summary_row(summary_data[4:])}</div>
    """

    found_section = generate_html_table_section(
        "✅ Data Ditemukan", cocok, display_columns,
        table_id="foundTable", table_class="success", status_text="Cocok"
    )

    not_found_section = generate_html_table_section(
        "❌ Data Tidak Ditemukan", tidak_cocok, display_columns,
        table_id="notFoundTable", table_class="danger", status_text="Tidak Cocok",
        dummy_if_empty=True
    )

    try:
        logging.info("Mengirim summary email ke: %s", ', '.join(report_recipient))
        send_summary_email(
            summary_data=summary_data,
            recipient_email=report_recipient,
            start_date=start_date,
            end_date=end_date
        )
        logging.info("Summary email berhasil dikirim.")
    except Exception as e:
        logging.error("❌ Gagal mengirim email: %s", str(e))


    html = f"""<!DOCTYPE html>
<html lang='id'>
<head>
    <meta charset='UTF-8'>
    <title>Laporan Perbandingan Transaksi Wadiah</title>
    <link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css' rel='stylesheet'>
    <link href='https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css' rel='stylesheet'/>
    <link href='https://fonts.googleapis.com/css2?family=Poppins&display=swap' rel='stylesheet'>
    <style>
        body {{ font-family: 'Poppins', sans-serif; background-color: #f0f8ff; padding: 30px; font-size: 0.75rem; }}
        .card-title {{ color: #0d6efd; font-weight: 600; }}
        .card-text {{ font-size: 1.2rem; }}
        .table-responsive {{ overflow-x: auto; }}
        .table th, .table td {{ vertical-align: middle; text-align: center; min-width: 120px; }}
        .section-header {{ margin-top: 40px; margin-bottom: 20px; }}
        .summary-row {{ display: flex; flex-wrap: wrap; justify-content: center; gap: 1rem; margin-bottom: 20px; }}
        .summary-card {{ flex: 1 1 200px; }}
    </style>
</head>
<body>
    <h2 class='mb-4 text-primary text-center'>Laporan Perbandingan Transaksi Wadiah</h2>
    {summary_cards_html}
    {found_section}
    {not_found_section}
    <script src='https://code.jquery.com/jquery-3.7.0.min.js'></script>
    <script src='https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js'></script>
    <script src='https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js'></script>
    <script>
        $(document).ready(function () {{
            $('#foundTable').DataTable({{ pageLength: 10, scrollX: true }});
            $('#notFoundTable').DataTable({{ pageLength: 10, scrollX: true }});
        }});
    </script>
</body>
</html>
"""
    

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)

