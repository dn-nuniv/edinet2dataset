# -*- coding: utf-8 -*-
"""
EDINETのTSV（type=5）を edinet2dataset の parser を使ってBS/PLに分解し、
template_ALL.csv の「項目」行に合わせて Y1..YN に CurrentYear の値を埋めたTSVを出力する。

使い方（例）:
  .\.venv\Scripts\python.exe .\scripts\22_export_by_template.py --edinet_code E03120 --years 5 --template .\template_ALL.csv
"""

import sys
import re
import ast
import subprocess
from pathlib import Path
import pandas as pd
import numpy as np

# --- 数値正規化 --------------------------------------------------------------

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

    # 既に数値ならそのまま
    if isinstance(val, (int, float)):
        if isinstance(val, float) and (val != val):  # NaN
            return None
        return float(val)

    s = str(val).strip()
    if s in DASHES:
        return None

    # カンマ除去
    s = s.replace(",", "")

    # 負数表現
    neg = False
    if s.startswith(("△", "▲")):
        neg = True
        s = s[1:].strip()
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()

    # 数字・記号以外を除去（念のため）
    s = re.sub(r"[^\d\.eE\+\-]", "", s).strip()
    if s in DASHES:
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
    return int(round(x))

def to_float_or_blank(val, ndigits=2):
    x = normalize_number(val)
    if x is None:
        return ""
    return round(float(x), ndigits)

# --- parser 呼び出し（カテゴリ別） -------------------------------------------

_PARSE_CACHE = {}

def parse_category(tsv_path: Path, category: str):
    """
    edinet2dataset の parser を subprocess で呼び出し、
    末尾に出る dict 表現を ast.literal_eval で復元して返す。

    戻り: { item: { period: value } }
    """
    key = (str(tsv_path), category)
    if key in _PARSE_CACHE:
        return _PARSE_CACHE[key]

    # venv python で repo 内の parser.py を直接叩く（環境混線回避）
    cmd = [
        sys.executable,
        "src/edinet2dataset/parser.py",
        "--file_path", str(tsv_path),
        "--category_list", category
    ]
    out = subprocess.check_output(cmd, text=True, errors="replace")

    lines = [l.strip() for l in out.splitlines() if l.strip().startswith("{") and l.strip().endswith("}")]
    if not lines:
        raise RuntimeError(f"parser output dict not found: {tsv_path} {category}")

    d = ast.literal_eval(lines[-1])
    _PARSE_CACHE[key] = d
    return d

def pick(d, candidates, period="CurrentYear"):
    """
    candidates の順に item を探し、見つかった最初の値を返す。
    """
    for k in candidates:
        if k in d and period in d[k]:
            return d[k][period]
    return None

def sum_vals(*vals):
    nums = [normalize_number(v) for v in vals]
    nums = [v for v in nums if v is not None]
    if not nums:
        return None
    return float(sum(nums))

# --- テンプレ項目 → 表記ゆれ吸収（最低限） ---------------------------------

# よくズレる項目だけ候補を持つ。テンプレに無いものは基本 item 名そのまま探す。
CAND = {
    "当期純利益": ["当期純利益", "当期利益", "親会社株主に帰属する当期純利益", "親会社株主に帰属する当期利益"],
    "現金預金": ["現金及び預金", "現金及び預金（流動資産）", "現金預金"],
    "売上債権（合計）": ["受取手形及び売掛金", "売掛金及び受取手形", "売上債権（合計）", "売上債権"],
    "仕入債務（合計）": ["支払手形及び買掛金", "買掛金及び支払手形", "仕入債務（合計）", "仕入債務"],
    "棚卸資産（合計）": ["棚卸資産（合計）", "棚卸資産"],
    "総資産": ["総資産", "資産合計"],
    "純資産": ["純資産"],
    "有価証券（流動資産）": ["有価証券（流動資産）", "有価証券"],
    "原材料": ["原材料", "原材料及び貯蔵品"],
    "貯蔵品": ["貯蔵品", "原材料及び貯蔵品"],
    "発行済株式数": ["発行済株式総数", "発行済株式数"],
    "EPS（1株当たり利益）": ["１株当たり当期純利益", "１株当たり当期利益"],
    "DPS（1株当たり配当）": ["１株当たり配当額"],
}

# どの表から取るか（基本）
FROM = {
    # PL
    "売上高": "PL",
    "売上原価": "PL",
    "営業利益": "PL",
    "営業外収益": "PL",
    "営業外費用": "PL",
    "支払利息": "PL",
    "当期純利益": "PL",
    "当期利益": "PL",

    # BS
    "流動資産": "BS",
    "流動負債": "BS",
    "現金預金": "BS",
    "売掛金": "BS",
    "受取手形": "BS",
    "電子記録債権": "BS",
    "買掛金": "BS",
    "支払手形": "BS",
    "電子記録債務": "BS",
    "有価証券（流動資産）": "BS",
    "棚卸資産（合計）": "BS",
    "商品": "BS",
    "製品": "BS",
    "原材料": "BS",
    "仕掛品": "BS",
    "半製品": "BS",
    "貯蔵品": "BS",
    "総資産": "BS",
    "純資産": "BS",
    "新株予約権": "BS",
    "非支配株主持分": "BS",
    "有利子負債": "BS",
    "固定負債": "BS",
    "有形固定資産": "BS",
    "売上債権（合計）": "BS",
    "仕入債務（合計）": "BS",

    # EDINET外（空で出す想定）
    "株価": "MANUAL",
    "EPS（1株当たり利益）": "OTHER",
    "DPS（1株当たり配当）": "OTHER",
    "発行済株式数": "OTHER",
}

FLOAT_ITEMS = {
    "EPS（1株当たり利益）",
    "DPS（1株当たり配当）",
    "株価",
}

def sort_ycols(cols):
    def key(c):
        m = re.match(r"Y(\d+)", c)
        return int(m.group(1)) if m else 999
    return sorted(cols, key=key)

# --- 本体 --------------------------------------------------------------------

def build_one_company(edinet_code: str, template_path: str, years: int, out_dir: str):
    data_dir = Path("data") / edinet_code
    if not data_dir.exists():
        raise SystemExit(f"data dir not found: {data_dir}")

    tpath = Path(template_path)
    if not tpath.exists():
        raise SystemExit(f"template not found: {tpath}")

    # テンプレ読み込み（文字列ベースにしておくとdtype事故が少ない）
    tpl = pd.read_csv(tpath, dtype="object")
    if "項目" not in tpl.columns:
        raise SystemExit("template の列に '項目' がありません")

    # Y列を特定
    ycols = [c for c in tpl.columns if isinstance(c, str) and c.startswith("Y")]
    ycols = sort_ycols(ycols)
    if not ycols:
        raise SystemExit("template に Y列（Y1..）がありません")

    # 代入で FutureWarning が出ないように object に統一
    out = tpl.copy()
    for c in ycols:
        out[c] = out[c].astype("object")

    # doc_id リスト（filings.csv があれば submitDateTime で古→新）
    filings = data_dir / "filings.csv"
    if filings.exists():
        f = pd.read_csv(filings, dtype="object")
        if "doc_id" not in f.columns:
            raise SystemExit("filings.csv に doc_id 列がありません")
        if "submitDateTime" in f.columns:
            f["submitDateTime"] = f["submitDateTime"].fillna("")
            f = f.sort_values("submitDateTime")
        doc_ids = f["doc_id"].astype(str).tolist()
        submit_map = dict(zip(f["doc_id"].astype(str), f.get("submitDateTime", pd.Series([""]*len(f))).astype(str)))
        desc_map = dict(zip(f["doc_id"].astype(str), f.get("docDescription", pd.Series([""]*len(f))).astype(str)))
    else:
        # TSVから
        doc_ids = sorted([p.stem for p in data_dir.glob("*.tsv")])
        submit_map = {d: "" for d in doc_ids}
        desc_map = {d: "" for d in doc_ids}

    if not doc_ids:
        raise SystemExit(f"TSVがありません: {data_dir}")

    use_years = min(years, len(doc_ids), len(ycols))
    use_ycols = ycols[:use_years]  # 左から詰める（Y1..）
    use_doc_ids = doc_ids[-use_years:]  # 末尾（新しい方）を使う：古→新の順で並ぶ前提

    # TSVパス
    tsv_paths = [data_dir / f"{d}.tsv" for d in use_doc_ids]
    for p in tsv_paths:
        if not p.exists():
            raise SystemExit(f"TSV not found: {p}")

    # パース（docごとにPL/BS）
    parsed = []
    for p in tsv_paths:
        pl = parse_category(p, "PL")
        bs = parse_category(p, "BS")
        parsed.append({"doc_id": p.stem, "PL": pl, "BS": bs})

    missing_items = []

    # 各テンプレ行を埋める
    for i, row in out.iterrows():
        item = str(row["項目"]).strip()

        # 空行などはスキップ
        if item == "" or item.lower() == "nan":
            continue

        candidates = CAND.get(item, [item])

        for yi, rec in zip(use_ycols, parsed):
            src = FROM.get(item, None)
            val = None

            if src == "PL":
                val = pick(rec["PL"], candidates)
            elif src == "BS":
                val = pick(rec["BS"], candidates)
            elif src in ("MANUAL", "OTHER"):
                val = None
            else:
                # 未定義は両方探す（保険）
                val = pick(rec["PL"], candidates)
                if val is None:
                    val = pick(rec["BS"], candidates)

            # --- 派生（合計が無いときに内訳を足す） ---
            if item == "棚卸資産（合計）" and val is None:
                # 商品/製品/原材料/仕掛品/半製品/貯蔵品 を足す
                val = sum_vals(
                    pick(rec["BS"], ["商品", "商品及び製品"]),
                    pick(rec["BS"], ["製品", "商品及び製品"]),
                    pick(rec["BS"], ["原材料", "原材料及び貯蔵品"]),
                    pick(rec["BS"], ["仕掛品"]),
                    pick(rec["BS"], ["半製品"]),
                    pick(rec["BS"], ["貯蔵品", "原材料及び貯蔵品"]),
                )

            if item == "売上債権（合計）" and val is None:
                val = sum_vals(
                    pick(rec["BS"], ["売掛金"]),
                    pick(rec["BS"], ["受取手形"]),
                    pick(rec["BS"], ["電子記録債権"]),
                )

            if item == "仕入債務（合計）" and val is None:
                val = sum_vals(
                    pick(rec["BS"], ["買掛金"]),
                    pick(rec["BS"], ["支払手形"]),
                    pick(rec["BS"], ["電子記録債務"]),
                )

            if item == "有利子負債" and val is None:
                val = sum_vals(
                    pick(rec["BS"], ["短期借入金"]),
                    pick(rec["BS"], ["1年内返済予定の長期借入金"]),
                    pick(rec["BS"], ["長期借入金"]),
                    pick(rec["BS"], ["社債"]),
                    pick(rec["BS"], ["リース債務"]),
                    pick(rec["BS"], ["リース債務（流動負債）"]),
                    pick(rec["BS"], ["リース債務（固定負債）"]),
                )

            # 代入（EPS等は float、他は int）
            if item in FLOAT_ITEMS:
                out.at[i, yi] = to_float_or_blank(val, ndigits=2)
            else:
                out.at[i, yi] = to_int_or_blank(val)

        # 欠損チェック（全部空なら missing に載せる。ただしMANUAL/OTHERは除外）
        if FROM.get(item, None) not in ("MANUAL", "OTHER"):
            filled = [out.at[i, c] for c in use_ycols]
            if all((x == "" or pd.isna(x)) for x in filled):
                missing_items.append(item)

    outp = Path(out_dir)
    outp.mkdir(parents=True, exist_ok=True)

    # 出力（テンプレ構造のままTSV）
    out_path = outp / f"{edinet_code}_tool_input.tsv"
    out.to_csv(out_path, sep="\t", index=False, encoding="utf-8-sig")

    # Y列とdoc_id対応（古→新）
    labels_path = outp / f"{edinet_code}_labels.tsv"
    lab = pd.DataFrame({
        "Y": use_ycols,
        "doc_id": [r["doc_id"] for r in parsed],
        "submitDateTime": [submit_map.get(r["doc_id"], "") for r in parsed],
        "docDescription": [desc_map.get(r["doc_id"], "") for r in parsed],
    })
    lab.to_csv(labels_path, sep="\t", index=False, encoding="utf-8-sig")

    # 取れなかった項目一覧
    miss_path = outp / f"{edinet_code}_missing_items.tsv"
    pd.DataFrame({"missing_items": sorted(set(missing_items))}).to_csv(miss_path, sep="\t", index=False, encoding="utf-8-sig")

    print("Saved:", out_path)
    print("Saved:", labels_path)
    print("Saved:", miss_path)
    print(f"Used years: {use_years}  doc_ids:", [r["doc_id"] for r in parsed])

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--edinet_code", required=True)
    p.add_argument("--template", default="template_ALL.csv")
    p.add_argument("--years", type=int, default=5)
    p.add_argument("--out_dir", default="work/export")
    args = p.parse_args()
    build_one_company(args.edinet_code, args.template, args.years, args.out_dir)

if __name__ == "__main__":
    main()
