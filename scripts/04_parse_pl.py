import sys
import subprocess
import ast
from pathlib import Path
import pandas as pd

def main():
    base = Path("data")
    candidates = []
    if (base / "E04707").exists():
        candidates = list((base / "E04707").glob("*.tsv"))
    if not candidates:
        candidates = list(base.rglob("*.tsv"))
    if not candidates:
        raise SystemExit("TSVが見つかりません。")

    tsv = candidates[0]
    py = sys.executable

    cmds = [
        [py, "-m", "edinet2dataset.parser", "--file_path", str(tsv), "--category_list", "PL"],
        [py, "src/edinet2dataset/parser.py", "--file_path", str(tsv), "--category_list", "PL"],
    ]

    out = None
    last_err = None
    for cmd in cmds:
        try:
            out = subprocess.check_output(cmd, text=True)
            break
        except Exception as e:
            last_err = e
    if out is None:
        raise SystemExit(f"parserの起動に失敗しました: {last_err}")

    lines = [l.strip() for l in out.splitlines() if l.strip().startswith("{") and l.strip().endswith("}")]
    if not lines:
        raise SystemExit("parser出力から辞書を抽出できませんでした。")

    d = ast.literal_eval(lines[-1])

    rows = []
    for item, periods in d.items():
        for period, val in periods.items():
            rows.append({"doc_id": tsv.stem, "statement": "PL", "item": item, "period": period, "value": val})

    df = pd.DataFrame(rows)
    df["value_num"] = pd.to_numeric(df["value"], errors="coerce")

    outdir = Path("work/olc/parsed")
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / "pl.parquet"
    df.to_parquet(outpath, index=False)

    print("Python:", py)
    print("TSV:", tsv)
    print("Saved:", outpath)
    print(df.head(10))

if __name__ == "__main__":
    main()

