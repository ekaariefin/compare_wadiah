import pandas as pd
from tqdm import tqdm

def parse_float(value):
    """Konversi string angka ke float, menangani format Indonesia."""
    if isinstance(value, str):
        value = value.replace('.', '').replace(',', '.').strip()
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def load_and_prepare_data(file1, file2, delimiter1=',', delimiter2 = ';'):
    """Membaca file CSV dan menyiapkan kolom nomor_seri."""
    df1 = pd.read_csv(file1, delimiter=delimiter1, dtype=str)
    df2 = pd.read_csv(file2, delimiter=delimiter2, dtype=str)

    if 'nomor_referensi' not in df1.columns:
        raise KeyError("Kolom 'nomor_referensi' tidak ditemukan di file 1")
    if df2.shape[1] < 2:
        raise KeyError("File 2 harus memiliki minimal 2 kolom")

    df1['nomor_seri'] = df1['nomor_referensi'].str[:23]
    df2 = df2.rename(columns={df2.columns[1]: 'nomor_seri'})
    df2['nomor_seri'] = df2['nomor_seri'].astype(str)

    return df1, df2

def compare_data(df1, df2):
    """Membandingkan data berdasarkan nomor_seri."""
    referensi_map = df1.set_index('nomor_seri')['nomor_referensi'].to_dict()
    cocok, tidak_cocok = [], []

    for _, row in tqdm(df2.iterrows(), total=len(df2), desc="\U0001F4CA Membandingkan"):
        row_dict = row.to_dict()
        nomor_seri = row_dict.get('nomor_seri', '')
        if nomor_seri in referensi_map:
            row_dict['nomor_referensi'] = referensi_map[nomor_seri]
            cocok.append(row_dict)
        else:
            row_dict['nomor_referensi'] = ''
            tidak_cocok.append(row_dict)

    return cocok, tidak_cocok

def calculate_debit_kredit(rows):
    """Menghitung total debit dan kredit dari list of dicts."""
    total_debit = sum(parse_float(row.get(' MUTASI DEBET ')) for row in rows)
    total_kredit = sum(parse_float(row.get(' MUTASI KREDIT ')) for row in rows)
    return total_debit, total_kredit

def render_rows(data, status, display_columns, dummy_if_empty=False):
    """Membuat baris HTML untuk tabel."""
    if data:
        return ''.join(
            f"<tr>{''.join(f'<td>{row.get(col, status if col == 'STATUS COMPARE' else '')}</td>' for col in display_columns)}</tr>"
            for row in data
        )
    elif dummy_if_empty:
        return f"<tr style='visibility:hidden;'>{''.join(f'<td>{col}</td>' for col in display_columns)}</tr>"
    else:
        return f"<tr><td colspan='{len(display_columns)}' class='text-center text-muted'>Tidak ada data</td></tr>"

def generate_html_table_section(title, data, display_columns, table_id, table_class, status_text='', dummy_if_empty=False):
    """Membuat section tabel HTML."""
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

def generate_html_report(df1, df2, cocok, tidak_cocok, filename):
    """Membangun dan menyimpan laporan HTML."""
    total_1, total_2 = len(df1), len(df2)
    total_found, total_not_found = len(cocok), len(tidak_cocok)

    display_columns = [
        'nomor_referensi', 'nomor_seri', 'KETERANGAN JURNAL',
        ' MUTASI DEBET ', ' MUTASI KREDIT ', 'STATUS COMPARE'
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

    summary_row_1 = summary_data[:4]
    summary_row_2 = summary_data[4:]

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
        <div class='summary-row'>{render_summary_row(summary_row_1)}</div>
        <div class='summary-row'>{render_summary_row(summary_row_2)}</div>
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
