#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""丹青社の古い年度のAR科目を確認"""
import polars as pl
from pathlib import Path

base = Path(r"C:\dev\edinet2dataset\edinet_corpus\annual\E00208")
tsvs = sorted(base.glob("*.tsv"))

ELEMENT_AR = [
    "NotesAndAccountsReceivableTrade",
    "NotesAndAccountsReceivableTradeAndContractAssets",
    "ReceivablesTradeAndContractAssets",
    "AccountsReceivableTrade",
    "TradeAndOtherReceivables",
    "NotesReceivableTrade",
    "ElectronicallyRecordedMonetaryClaimsOperating",
    "AccountsReceivableOther",
]

print("=== E00208 AR availability by TSV ===\n")

for tsv in tsvs[:3] + tsvs[-2:]:  # 古い3つ + 新しい2つ
    df = pl.read_csv(tsv, separator="\t", encoding="utf-16", 
                     ignore_errors=True, truncate_ragged_lines=True, infer_schema_length=0)
    
    # 会計年度取得
    fy_start = df.filter(pl.col("要素ID").str.contains("CurrentFiscalYearStartDateDEI"))
    fy = fy_start["値"][0][:4] if fy_start.height > 0 else "?"
    
    print(f"{tsv.name} (FY{fy}):")
    found_any = False
    for elem in ELEMENT_AR:
        full_elem = f"jppfs_cor:{elem}"
        matches = df.filter(
            (pl.col("要素ID") == full_elem) 
            & (pl.col("相対年度") == "当期末")
            & (pl.col("連結・個別") == "連結")
        )
        if matches.height > 0:
            print(f"  ✓ {elem}: {matches['値'][0]}")
            found_any = True
    if not found_any:
        print(f"  ✗ No AR elements found in 連結")
        # 代替を探す
        all_ar = df.filter(
            pl.col("要素ID").str.contains("jppfs_cor:")
            & (pl.col("要素ID").str.to_lowercase().str.contains("receiv") | 
               pl.col("項目名").str.contains("売掛") |
               pl.col("項目名").str.contains("手形"))
            & (pl.col("相対年度") == "当期末")
            & (pl.col("連結・個別") == "連結")
        )
        if all_ar.height > 0:
            for row in all_ar.iter_rows(named=True):
                print(f"    -> {row['要素ID']}: {row['項目名']} = {row['値']}")
    print()
