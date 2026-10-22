import re, base64, os, glob

# cari index.html di proyek
paths = glob.glob("**/index.html", recursive=True)
paths = [p for p in paths if "venv" not in p]
print("Ditemukan:", paths)
src = paths[0]

html = open(src, encoding="utf-8").read()
m = re.search(r'url\("data:image/jpeg;base64,([^"]+)"\)', html)
if not m:
    raise SystemExit("Gambar base64 tidak ditemukan di " + src)

os.makedirs("static/img", exist_ok=True)
open("static/img/wash.jpg", "wb").write(base64.b64decode(m.group(1)))
print("Selesai: static/img/wash.jpg")
