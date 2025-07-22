from utils import load_and_prepare_data, compare_and_export_csv, send_email_with_attachments
from datetime import datetime
import logging
import os
from io import StringIO


logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def analyzed(batch_id, start_date, end_date):
    print(f"🔄 Membaca file untuk batch ID: {batch_id}...")

    file_oracle = f"data/oracle_{batch_id}.csv"
    file_pg = f"data/pg_{batch_id}.csv"

    print(f"🔍 Membaca file {file_oracle} dan {file_pg}")
    df_oracle, df_pg = load_and_prepare_data(file_oracle, file_pg, delimiter1=';', delimiter2=';')

    print("📊 Membandingkan data PG → Oracle dan Oracle → PG berdasarkan Nomor Referensi...")
    df_oracle_to_pg, df_pg_to_oracle, output_pg_to_oracle, output_oracle_to_pg, df_all_unmatched = compare_and_export_csv(
        df_pg, df_oracle, batch_id
    )
    print(f"PG TO ORA: {df_pg_to_oracle.columns.tolist()})")
    print(f"ORA TO PG: {df_oracle_to_pg.columns.tolist()})")


    print(f"✅ Jumlah data PG → Oracle: {len(df_pg_to_oracle)}")
    print(f"✅ Jumlah data Oracle → PG: {len(df_oracle_to_pg)}")

    print("📁 File hasil perbandingan disimpan:")
    print(f"   - {output_pg_to_oracle}")
    print(f"   - {output_oracle_to_pg}")

    attachments = [output_pg_to_oracle, output_oracle_to_pg]

    df_all_unmatched.drop_duplicates()

    # Pastikan df_all_unmatched adalah DataFrame yang valid
    if not df_all_unmatched.empty:
        # Buat isi tabel baris demi baris
        rows_html = ""
        for idx, row in df_all_unmatched.iterrows():
            rows_html += f"""
                <tr>
                    <td>{row['Nomor_Referensi']}</td>
                    <td>{row['Status Compare']}</td>
                </tr>
            """
    else:
        rows_html = """
            <tr>
                <td colspan="3" style="text-align: center;">Tidak ada data yang tidak cocok</td>
            </tr>
        """

    # Gabungkan semua ke dalam template HTML body
    body = f"""
        <p><b>Daftar Transaksi Tidak Cocok: {len(df_all_unmatched)}</b></p>
        <table border="1" cellpadding="6" cellspacing="0" style="border-collapse: collapse; font-family: Arial, sans-serif; font-size: 14px; width: 100%;">
            <thead>
                <tr style="background-color: #f2f2f2;">
                    <th>Nomor Referensi</th>
                    <th>Status Compare</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    """
    send_email_with_attachments(start_date, end_date, batch_id=batch_id, attachments=attachments, summary_text=body)