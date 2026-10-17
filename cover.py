"""
Custom Cover (fitur Premium): validasi gambar upload + sematkan ke file MP3.

Dependensi: pillow, mutagen
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass
from pathlib import Path

from fastapi import UploadFile
from mutagen import MutagenError
from mutagen.id3 import APIC, ID3, ID3NoHeaderError
from mutagen.mp3 import MP3
from PIL import Image, ImageOps, UnidentifiedImageError

log = logging.getLogger("spotify-zipper.cover")

# --------------------------------------------------------------------------- #
# Batasan
# --------------------------------------------------------------------------- #
MAX_BYTES = 2 * 1024 * 1024        # 2 MB
MAX_PIXELS = 25_000_000            # tolak "decompression bomb"
MIN_SIDE = 200                     # px, terlalu kecil = cover jelek
MAX_SIDE = 1400                    # px, hasil di-resize kalau lebih besar
ALLOWED_FORMATS = {"JPEG", "PNG"}

JPEG_MAGIC = b"\xff\xd8\xff"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"

Image.MAX_IMAGE_PIXELS = MAX_PIXELS


class CoverError(Exception):
    """Error yang pesannya aman ditampilkan ke user."""


@dataclass
class ValidatedCover:
    data: bytes                    # JPEG bersih hasil re-encode
    mime: str = "image/jpeg"


# --------------------------------------------------------------------------- #
# 1. Validasi gambar
# --------------------------------------------------------------------------- #
def validate_cover_bytes(raw: bytes) -> ValidatedCover:
    """Validasi sinkron (CPU-bound). Jalankan lewat asyncio.to_thread."""
    if not raw:
        raise CoverError("File gambar kosong.")
    if len(raw) > MAX_BYTES:
        raise CoverError("Ukuran gambar maksimal 2 MB.")

    # Cek magic bytes (jangan percaya ekstensi / Content-Type dari client)
    if not (raw.startswith(JPEG_MAGIC) or raw.startswith(PNG_MAGIC)):
        raise CoverError("Format gambar harus JPG atau PNG.")

    try:
        # verify() hanya memeriksa integritas; setelahnya objek tidak bisa dipakai lagi
        with Image.open(io.BytesIO(raw)) as probe:
            fmt = probe.format
            width, height = probe.size
            probe.verify()
    except (UnidentifiedImageError, Image.DecompressionBombError, OSError, SyntaxError, ValueError):
        raise CoverError("File bukan gambar yang valid atau rusak.")

    if fmt not in ALLOWED_FORMATS:
        raise CoverError("Format gambar harus JPG atau PNG.")
    if width * height > MAX_PIXELS:
        raise CoverError("Resolusi gambar terlalu besar.")
    if min(width, height) < MIN_SIDE:
        raise CoverError(f"Resolusi gambar minimal {MIN_SIDE}x{MIN_SIDE} piksel.")

    # Buka ulang, lalu RE-ENCODE ke JPEG baru. Ini membuang EXIF/metadata dan
    # payload tersembunyi (polyglot, data setelah EOF, chunk aneh), jadi yang
    # disematkan ke MP3 adalah piksel murni.
    try:
        with Image.open(io.BytesIO(raw)) as img:
            img.load()
            img = ImageOps.exif_transpose(img)  # hormati orientasi foto HP

            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                rgba = img.convert("RGBA")
                bg = Image.new("RGB", rgba.size, (255, 255, 255))
                bg.paste(rgba, mask=rgba.getchannel("A"))
                img = bg
            else:
                img = img.convert("RGB")

            img.thumbnail((MAX_SIDE, MAX_SIDE), Image.Resampling.LANCZOS)

            out = io.BytesIO()
            img.save(out, format="JPEG", quality=90, optimize=True)  # tanpa exif
    except (OSError, ValueError, Image.DecompressionBombError):
        raise CoverError("Gambar gagal diproses. Coba gambar lain.")

    return ValidatedCover(data=out.getvalue())


async def validate_cover_upload(upload: UploadFile) -> ValidatedCover:
    """Baca UploadFile (maks 2MB + 1 byte) lalu validasi."""
    import asyncio

    raw = await upload.read(MAX_BYTES + 1)  # jangan baca tanpa batas
    return await asyncio.to_thread(validate_cover_bytes, raw)


# --------------------------------------------------------------------------- #
# 2. Embed cover ke MP3
# --------------------------------------------------------------------------- #
def embed_cover(mp3_path: str | Path, image_bytes: bytes, mime: str = "image/jpeg") -> None:
    """Hapus semua cover lama (termasuk dari Spotify), lalu sematkan gambar baru."""
    path = Path(mp3_path)
    if path.suffix.lower() != ".mp3":
        raise CoverError(f"{path.name}: custom cover hanya didukung untuk MP3.")
    if not image_bytes:
        raise CoverError("image_bytes kosong, tidak ada yang bisa disematkan.")

    try:
        MP3(path)  # pastikan benar-benar file MP3
    except MutagenError as exc:
        raise CoverError(f"{path.name}: bukan file MP3 yang valid.") from exc

    try:
        tags = ID3(path)
    except ID3NoHeaderError:
        tags = ID3()  # file belum punya tag ID3

    old_count = len(tags.getall("APIC"))
    tags.delall("APIC")
    tags.add(
        APIC(
            encoding=3,        # UTF-8 (mutagen otomatis konversi ke UTF-16 untuk ID3v2.3)
            mime=mime,
            type=3,            # 3 = Cover (front)
            desc="Cover",
            data=image_bytes,
        )
    )
    tags.save(path, v2_version=3)

    # --- Verifikasi: baca ulang dari disk ---
    saved = ID3(path).getall("APIC")
    if len(saved) != 1 or saved[0].data != image_bytes:
        raise CoverError(f"{path.name}: verifikasi gagal, cover tidak tersimpan di file.")

    print(
        f"[COVER] OK  {path.name} | APIC lama dihapus: {old_count} | "
        f"baru: {len(image_bytes)} byte ({mime})",
        flush=True,
    )


def embed_cover_in_folder(folder: str | Path, image_bytes: bytes, mime: str = "image/jpeg") -> tuple[int, int]:
    """Sematkan cover ke semua MP3 di folder. Return (berhasil, gagal). Blocking."""
    folder = Path(folder)
    all_files = sorted(p.name for p in folder.iterdir()) if folder.is_dir() else []
    mp3_files = sorted(p for p in folder.glob("*") if p.suffix.lower() == ".mp3")  # case-insensitive
    print(
        f"[COVER] Folder {folder} | total file: {len(all_files)} | MP3: {len(mp3_files)} | "
        f"ukuran cover: {len(image_bytes)} byte",
        flush=True,
    )
    if not mp3_files:
        print(f"[COVER] Tidak ada MP3. Isi folder: {all_files}", flush=True)

    ok = failed = 0
    for mp3 in mp3_files:
        try:
            embed_cover(mp3, image_bytes, mime)
            ok += 1
        except (CoverError, MutagenError, OSError) as exc:
            print(f"[COVER] GAGAL {mp3.name}: {exc!r}", flush=True)
            log.warning("Gagal menyematkan cover ke %s: %s", mp3.name, exc)
            failed += 1
    print(f"[COVER] Selesai: {ok} berhasil, {failed} gagal", flush=True)
    return ok, failed