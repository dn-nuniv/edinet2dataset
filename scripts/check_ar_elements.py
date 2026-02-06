#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""売掛金関連の要素ID調査スクリプト"""
import polars as pl
from pathlib import Path

# サンプルTSV
tsv_paths = [
    r"C:\dev\edinet2dataset\edinet_corpus\annual\E00208\S100VNG9.tsv",  # 丹青社（サービス業）
    r"C:\dev\edinet2dataset\edinet_corpus\annual\E00011\S100VIRG.tsv",  # 住友林業
]

for tsv_path in tsv_paths:
    print(f"\n=== {Path(tsv_path).parent.name} / {Path(tsv_path).name} ===")
    df = pl.read_csv(tsv_path, separator="\t", encoding="utf-16", 
                     ignore_errors=True, truncate_ragged_lines=True, infer_schema_length=0)
    
    # jppfs_cor で Receivable または 手形/債権/売掛 を含む
    ar_related = df.filter(
        pl.col("要素ID").str.contains("jppfs_cor:") 
        & (
            pl.col("要素ID").str.to_lowercase().str.contains("receiv")
            | pl.col("要素ID").str.to_lowercase().str.contains("note")
            | pl.col("要素ID").str.to_lowercase().str.contains("electronic")
        )
        & (pl.col("連結・個別").is_in(["連結", "個別"]))
        & (pl.col("相対年度").is_in(["当期末", "前期末"]))
    )
    
    unique_elems = ar_related.select(["要素ID", "項目名"]).unique()
    for row in unique_elems.iter_rows(named=True):
        print(f"  {row['要素ID']}")
        print(f"    -> {row['項目名']}")
