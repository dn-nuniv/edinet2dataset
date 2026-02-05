from __future__ import annotations

import argparse
import ast
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ---------------------------
# 数値正規化
# ---------------------------

DASHES = {"", "－", "―", "ー", "-", "–", "—"}  # EDINETでよく出る「該当なし」表現


def normalize_number(val):
    """
    EDINETの値（数値/文字列）→ float or None に正規化
    - '－' 等は None
    - '△123' '(123)' は負数
    - '1,234' はカンマ除去
    """
    if val is None:
        return None

    if isinstance(val, (int, float)):
        if isinstance(val, float) and (val != val):  # NaN
            return None
        return float(val)

    s = str(val).strip()
    if s in DASHES:
        return None

    s = s.replace(",", "")

    neg = False
    if s.startswith(("△", "▲")):
        neg = True
        s = s[1:].strip()
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    s = re.sub(r"[^\d\.eE\+\-]", "", s).strip()
    if s in DASHES or s == "":
        return None

    try:
        x = float(s)
    except Exception:
        return None

    return -x if neg else x


def to_int_or_blank(val):
    x = normalize_number(val)
    if x is None:
        return ""
    # 基本「金額系」が多い前提で int に寄せる（学生配布向き）
    return int(round(x))


# ---------------------------
# edinet2dataset parser 呼び出し
# ---------------------------

_PARSE_CACHE: Dict[Tuple[str, str], Dict] = {}


def _resolve_parser_path() -> Path:
    """
    scripts/ 直下に置かれる想定で、repo root を推定して parser.py を探す。
    見つからなければ cwd 相対の src/... を使う。
    """
    here = Path(__file__).resolve()
    repo_root = here.parent.parent  # scripts/.. = repo root 想定
    cand = repo_root / "src" / "edinet2dataset" / "parser.py"
    if cand.exists():
        return cand
    cand2 = Path("src") / "edinet2dataset" / "parser.py"
    return cand2


PARSER_PATH = _resolve_parser_path()


def parse_category(tsv_path: Path, category: str) -> Dict:
    """
    edinet2dataset の parser を subprocess で呼び出し、
    末尾に出る dict 表現を ast.literal_eval で復元して返す。

    戻り: { item: { period: value } }
    """
    key = (str(tsv_path), category)
    if key in _PARSE_CACHE:
        return _PARSE_CACHE[key]

    if not PARSER_PATH.exists():
        raise RuntimeError(
            f"parser.py not found: {PARSER_PATH}\n"
            f"repo直下で実行しているか確認してください（src/edinet2dataset/parser.py が必要）"
        )

    cmd = [
        sys.executable,
        str(PARSER_PATH),
        "--file_path",
        str(tsv_path),
        "--category_list",
        category,
    ]
    out = subprocess.check_output(cmd, text=True, errors="replace")

    lines = [
        l.strip()
        for l in out.splitlines()
        if l.strip().startswith("{") and l.strip().endswith("}")
    ]
    if not lines:
        raise RuntimeError(f"parser output dict not found: {tsv_path} {category}")

    d = ast.literal_eval(lines[-1])
    _PARSE_CACHE[key] = d
    return d


# ---------------------------
# JSONメタ読取・doc選別
# ---------------------------

def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = str(s).strip()
    fmts = ["%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"]
    for f in fmts:
        try:
            return datetime.strptime(s, f)
        except Exception:
            pass
    return None


@dataclass(frozen=True)
class FilingMeta:
    doc_id: str
    period_end: str
    submit_dt: str
    desc: str
    json_path: Path
    tsv_path: Path


def read_json_metas(data_dir: Path) -> List[FilingMeta]:
    metas: List[FilingMeta] = []
    for jp in data_dir.glob("*.json"):
        try:
            obj = json.loads(jp.read_text(encoding="utf-8"))
        except Exception:
            try:
                obj = json.loads(jp.read_text(encoding="utf-8-sig"))
            except Exception:
                continue

        doc_id = str(obj.get("docID") or jp.stem).strip()
        period_end = str(obj.get("periodEnd") or "").strip()
        submit_dt = str(obj.get("submitDateTime") or "").strip()
        desc = str(obj.get("docDescription") or "").strip()

        tsvp = data_dir / f"{doc_id}.tsv"
        if not tsvp.exists():
            # jsonはあるがtsvが無い（DL途中など）ならスキップ
            continue

        metas.append(
            FilingMeta(
                doc_id=doc_id,
                period_end=period_end,
                submit_dt=submit_dt,
                desc=desc,
                json_path=jp,
                tsv_path=tsvp,
            )
        )
    return metas


def select_latest_per_period(metas: List[FilingMeta]) -> List[FilingMeta]:
    """
    同じ period_end が複数ある（訂正など）場合は、submitDateTime が最新のものを採用。
    """
    by_period: Dict[str, List[FilingMeta]] = {}
    for m in metas:
        if not m.period_end:
            continue
        by_period.setdefault(m.period_end, []).append(m)

    chosen: List[FilingMeta] = []
    for pend, arr in by_period.items():
        arr_sorted = sorted(
            arr,
            key=lambda x: (_parse_dt(x.submit_dt) or datetime.min, x.doc_id),
        )
        chosen.append(arr_sorted[-1])  # 最新
    # period_end 昇順
    chosen.sort(key=lambda x: x.period_end)
    return chosen


def take_last_n_periods(metas_unique: List[FilingMeta], years: int) -> List[FilingMeta]:
    if years <= 0:
        return metas_unique
    if len(metas_unique) <= years:
        return metas_unique
    return metas_unique[-years:]


# ---------------------------
# ダンプ（PL/BS 全項目）
# ---------------------------

def union_items(parsed_list: List[Dict]) -> List[str]:
    items = set()
    for d in parsed_list:
        items |= set(d.keys())
    return sorted(items)


def build_wide_df(parsed_list: List[Dict], col_labels: List[str]) -> pd.DataFrame:
    """
    parsed_list: [{item: {CurrentYear: val, ...}}, ...] が years 個
    col_labels:  years 個（例: periodEnd）
    """
    items = union_items(parsed_list)
    rows = []
    for item in items:
        row = {"項目": item}
        for d, col in zip(parsed_list, col_labels):
            val = None
            if item in d and isinstance(d[item], dict):
                val = d[item].get("CurrentYear")
            row[col] = to_int_or_blank(val)
        rows.append(row)
    return pd.DataFrame(rows)


def export_one_company(
    edinet_code: str,
    corpus_root: Path,
    years: int,
    out_dir: Path,
) -> Dict[str, Path]:
    data_dir = corpus_root / edinet_code
    if not data_dir.exists():
        raise SystemExit(f"data dir not found: {data_dir}")

    metas = read_json_metas(data_dir)
    if not metas:
        raise SystemExit(f"json+tsv が見つかりません: {data_dir}")

    unique = select_latest_per_period(metas)
    use = take_last_n_periods(unique, years)

    if not use:
        raise SystemExit(f"対象データが空です: {data_dir}")

    # 列名は periodEnd（ユニーク）
    col_labels = [m.period_end for m in use]

    # パース
    pl_list = []
    bs_list = []
    for m in use:
        pl = parse_category(m.tsv_path, "PL")
        bs = parse_category(m.tsv_path, "BS")
        pl_list.append(pl)
        bs_list.append(bs)

    out_dir.mkdir(parents=True, exist_ok=True)

    pl_df = build_wide_df(pl_list, col_labels)
    bs_df = build_wide_df(bs_list, col_labels)

    pl_path = out_dir / f"{edinet_code}_PL_all.tsv"
    bs_path = out_dir / f"{edinet_code}_BS_all.tsv"
    pl_df.to_csv(pl_path, sep="\t", index=False, encoding="utf-8-sig")
    bs_df.to_csv(bs_path, sep="\t", index=False, encoding="utf-8-sig")

    labels_path = out_dir / f"{edinet_code}_labels.tsv"
    lab = pd.DataFrame(
        {
            "col": col_labels,
            "doc_id": [m.doc_id for m in use],
            "periodEnd": [m.period_end for m in use],
            "submitDateTime": [m.submit_dt for m in use],
            "docDescription": [m.desc for m in use],
            "tsv_path": [str(m.tsv_path) for m in use],
        }
    )
    lab.to_csv(labels_path, sep="\t", index=False, encoding="utf-8-sig")

    print(f"[{edinet_code}] Saved: {pl_path}")
    print(f"[{edinet_code}] Saved: {bs_path}")
    print(f"[{edinet_code}] Saved: {labels_path}")
    print(f"[{edinet_code}] Used periods: {col_labels}")

    return {"pl": pl_path, "bs": bs_path, "labels": labels_path}


def parse_codes(args) -> List[str]:
    codes = []
    if args.edinet_code:
        codes.append(args.edinet_code.strip())
    if args.edinet_codes:
        for x in str(args.edinet_codes).split(","):
            x = x.strip()
            if x:
                codes.append(x)
    # 重複除去（順序維持）
    out = []
    seen = set()
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    if not out:
        raise SystemExit("edinet_code を指定してください（--edinet_code または --edinet_codes）")
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--edinet_code", default=None, help="単一のEDINETコード（例: E03120）")
    p.add_argument("--edinet_codes", default=None, help="カンマ区切り複数（例: E03120,E03133）")
    p.add_argument("--years", type=int, default=6, help="何期ぶん出すか（unique periodEnd ベース）")
    p.add_argument(
        "--corpus_root",
        default=r"C:\dev\edinet2dataset\edinet_corpus\annual",
        help=r"コーパスルート（既定: C:\dev\edinet2dataset\edinet_corpus\annual）",
    )
    p.add_argument("--out_dir", default="work/dump", help="出力先")
    p.add_argument("--no_combined", action="store_true", help="複数社でも combined_long.tsv を作らない")

    args = p.parse_args()

    codes = parse_codes(args)
    corpus_root = Path(args.corpus_root)
    out_dir = Path(args.out_dir)

    combined_rows = []

    for code in codes:
        paths = export_one_company(code, corpus_root, args.years, out_dir)

        if not args.no_combined:
            # 会社ごとにPL/BSを long にして結合（学生がピボットしやすい）
            for stmt, fp in [("PL", paths["pl"]), ("BS", paths["bs"])]:
                df = pd.read_csv(fp, sep="\t", dtype="object")
                # wide -> long
                long = df.melt(id_vars=["項目"], var_name="periodEnd", value_name="value")
                long.insert(0, "statement", stmt)
                long.insert(0, "edinet_code", code)
                combined_rows.append(long)

    if combined_rows and not args.no_combined:
        comb = pd.concat(combined_rows, ignore_index=True)
        comb_path = out_dir / "combined_long.tsv"
        comb.to_csv(comb_path, sep="\t", index=False, encoding="utf-8-sig")
        print("Saved:", comb_path)


if __name__ == "__main__":
    main()