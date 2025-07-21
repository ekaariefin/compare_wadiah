import subprocess
import os

# Nama folder untuk menyimpan paket offline
offline_dir = "offline_packages"
os.makedirs(offline_dir, exist_ok=True)

# File requirements
requirements_file = "requirements.txt"

# Tools build penting
build_tools = ["setuptools>=40.8.0", "wheel", "build"]

# Gabungkan tools + isi file requirements
full_requirements = []

# Tambahkan tools build
full_requirements.extend(build_tools)

# Baca isi requirements.txt
with open(requirements_file, "r") as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#"):
            full_requirements.append(line)

# Tampilkan daftar paket yang akan di-download
print("\n📦 Daftar paket yang akan diunduh:")
for pkg in full_requirements:
    print(" -", pkg)

# Jalankan pip download
print("\n🚀 Mendownload paket ke folder:", offline_dir)
try:
    subprocess.run(
        ["pip", "download", "-d", offline_dir] + full_requirements,
        check=True
    )
    print("\n✅ Semua paket berhasil diunduh.")
except subprocess.CalledProcessError as e:
    print("\n❌ Gagal mendownload paket:", e)
