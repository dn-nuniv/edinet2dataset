import sys
import subprocess
import ast
from pathlib import Path
import pandas as pd

def parse_one(tsv_path: Path, category: str):
    py = sys.executable
    cmds = [
        [py, "-m", "edinet2dataset.parser", "--file_path", str(tsv_path), "--category_list", category],
        [py, "src/edinet2dataset/parser.py", "--file_path", str(tsv_path), "--category_list", category],
    ]
    out = None
    last_err = None
    for cmd in cmds:
        try:
            out = subprocess.check_output(cmd, text=True)  # ★encoding指定しない（文字化け防止）
            break
        except Exception as e:
            last_err = e
    if out is None:
        raise RuntimeError(f"parser failed: {last_err}")

    lines = [l.strip() for l in out.splitlines() if l.strip().startswith("{") and l.strip().endswith("}")]
    if not lines:
        raise RuntimeError("parser output did not contain dict-like line")
    d = ast.literal_eval(lines[-1])

    rows = []
    for item, periods in d.items():
        for period, val in periods.items():
            rows.append({
                "doc_id": tsv_path.stem,
                "statement": category,
                "item": item,
                "period": period,
                "value": val,
            })

    df = pd.DataFrame(rows)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")
    return df

def main():
    tsvs = sorted(Path("data/E04707").glob("*.tsv"))
    if not tsvs:
        raise SystemExit("data/E04707 にTSVがありません。先にダウンロードしてください。")

    outdir = Path("work/olc/parsed")
    outdir.mkdir(parents=True, exist_ok=True)

    for tsv in tsvs:
        for cat in ["BS", "PL"]:
            df = parse_one(tsv, cat)
            outpath = outdir / f"{cat.lower()}_{tsv.stem}.parquet"
            df.to_parquet(outpath, index=False)
            print("Saved:", outpath, "rows=", len(df))

if __name__ == "__main__":
    main()
