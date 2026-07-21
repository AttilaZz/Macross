"""
Découpe un gros CSV (zippé/gzippé) stocké sur GCS en fichiers de 3 mois (trimestres),
puis réécrit chaque fichier sur GCS en csv.gz.

Fonctionnement : lecture en streaming par chunks pandas -> jamais 1 Go en RAM.
Écriture en streaming directement sur GCS (pas de disque local requis).

Usage :
    python split_gcs_csv_by_quarter.py \
        --input gs://mon-bucket/path/gros_fichier.csv.zip \
        --output-prefix gs://mon-bucket/path/split/ \
        --date-col date_jour

Dépendances : pip install pandas gcsfs
"""

import argparse
import gzip
import io
import zipfile

import gcsfs
import numpy as np
import pandas as pd

CHUNKSIZE = 200_000  # lignes par chunk, à ajuster selon la RAM


def build_windows(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DatetimeIndex:
    """Construit les bornes de fenêtres de 3 mois en remontant depuis end_date
    jusqu'à couvrir start_date. Retourne les bornes triées croissantes."""
    bounds = [end_date]
    cur = end_date
    while cur > start_date:
        cur = cur - pd.DateOffset(months=3)
        bounds.append(cur)
    return pd.DatetimeIndex(sorted(bounds))


def open_input_stream(fs: gcsfs.GCSFileSystem, path: str):
    """Retourne un flux texte sur le CSV, que le fichier soit .zip, .gz ou .csv brut."""
    raw = fs.open(path, "rb")
    if path.endswith(".zip"):
        zf = zipfile.ZipFile(raw)
        inner_name = zf.namelist()[0]  # on suppose 1 seul CSV dans le zip
        print(f"Fichier interne du zip : {inner_name}")
        return io.TextIOWrapper(zf.open(inner_name), encoding="utf-8")
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.GzipFile(fileobj=raw), encoding="utf-8")
    return io.TextIOWrapper(raw, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="gs://bucket/path/fichier.csv.zip")
    parser.add_argument("--output-prefix", required=True, help="gs://bucket/path/split/")
    parser.add_argument("--date-col", required=True, help="Nom de la colonne date")
    parser.add_argument("--sep", default=",", help="Séparateur CSV (défaut ,)")
    parser.add_argument("--date-format", default=None, help="Format date optionnel, ex %%Y-%%m-%%d")
    parser.add_argument("--start", default="2025-01-01", help="Date min couverte (défaut 2025-01-01)")
    parser.add_argument("--end", default=None, help="Date max / ancre des fenêtres (défaut aujourd'hui)")
    args = parser.parse_args()

    fs = gcsfs.GCSFileSystem()
    output_prefix = args.output_prefix.rstrip("/")

    start_date = pd.Timestamp(args.start)
    end_date = pd.Timestamp(args.end) if args.end else pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
    bounds = build_windows(start_date, end_date)
    labels = [
        f"{bounds[i].date()}_{(bounds[i + 1] - pd.Timedelta(days=1)).date()}"
        for i in range(len(bounds) - 1)
    ]
    print("Fenêtres de 3 mois (depuis aujourd'hui en remontant) :")
    for lb in labels:
        print(f"  - {lb}")

    # writers ouverts par fenêtre : {"2026-04-22_2026-07-21": gzip_writer}
    writers: dict[str, gzip.GzipFile] = {}
    gcs_files: dict[str, object] = {}

    def get_writer(period: str) -> gzip.GzipFile:
        if period not in writers:
            out_path = f"{output_prefix}/{period}.csv.gz"
            print(f"Ouverture writer -> {out_path}")
            f = fs.open(out_path, "wb")
            gcs_files[period] = f
            writers[period] = gzip.GzipFile(fileobj=f, mode="wb")
        return writers[period]

    headers_written: set[str] = set()
    total_rows = 0

    stream = open_input_stream(fs, args.input)
    reader = pd.read_csv(stream, sep=args.sep, chunksize=CHUNKSIZE, dtype=str)

    for i, chunk in enumerate(reader):
        dates = pd.to_datetime(chunk[args.date_col], format=args.date_format, errors="coerce")
        n_bad = dates.isna().sum()
        if n_bad:
            print(f"  ATTENTION : {n_bad} lignes avec date invalide dans le chunk {i} (ignorées)")
            chunk = chunk[dates.notna()]
            dates = dates[dates.notna()]

        # affectation de chaque ligne à sa fenêtre de 3 mois
        idx = np.searchsorted(bounds.values, dates.values, side="right") - 1
        in_range = (idx >= 0) & (idx < len(labels))
        n_out = (~in_range).sum()
        if n_out:
            print(f"  {n_out} lignes hors plage [{start_date.date()} ; {end_date.date()}[ ignorées (chunk {i})")
        chunk = chunk[in_range]
        periods = pd.Series(idx[in_range], index=chunk.index).map(lambda k: labels[k])

        for period, sub in chunk.groupby(periods):
            w = get_writer(period)
            buf = io.StringIO()
            sub.to_csv(buf, sep=args.sep, index=False, header=period not in headers_written)
            headers_written.add(period)
            w.write(buf.getvalue().encode("utf-8"))

        total_rows += len(chunk)
        print(f"Chunk {i} traité — {total_rows:,} lignes cumulées")

    # fermeture propre : flush gzip puis flush GCS
    for period in writers:
        writers[period].close()
        gcs_files[period].close()
        print(f"Finalisé : {output_prefix}/{period}.csv.gz")

    print(f"Terminé. {total_rows:,} lignes réparties sur {len(writers)} fichiers trimestriels.")


if __name__ == "__main__":
    main()
