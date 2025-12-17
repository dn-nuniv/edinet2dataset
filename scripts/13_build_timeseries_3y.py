import pandas as pd
import numpy as np

df = pd.read_csv("work/olc/mart_by_doc_period.csv")

# 新しい有報 = CurrentYearの売上が最大のdoc_id（簡易だが今回は確実）
cur = df[df["period"]=="CurrentYear"].copy()
new_doc = cur.sort_values("sales", ascending=False).iloc[0]["doc_id"]

# 年0（当期）と年-1（前期）
y0 = df[(df.doc_id==new_doc) & (df.period=="CurrentYear")].iloc[0]
y1 = df[(df.doc_id==new_doc) & (df.period=="Prior1Year")].iloc[0]

# 1つ前の有報 = CurrentYearの売上が y1.sales と一致するdoc_id
# （浮動小数の誤差もあるので isclose）
cand = df[(df.period=="CurrentYear") & (df.doc_id!=new_doc)].copy()
cand["match"] = np.isclose(cand["sales"], y1["sales"], rtol=0, atol=0.5)  # 0.5円誤差許容（実質ぴったりのはず）
prev_docs = cand[cand["match"]]["doc_id"].unique()

if len(prev_docs) == 0:
    raise SystemExit("前期で重なるdoc_idが見つかりませんでした（2本目の有報が別期間かも）")
if len(prev_docs) > 1:
    print("WARNING: 候補が複数あります。最初の候補を使います:", prev_docs)

prev_doc = prev_docs[0]

# 年-2（前々期）= 1つ前の有報の Prior1Year
y2 = df[(df.doc_id==prev_doc) & (df.period=="Prior1Year")].iloc[0]

out = pd.DataFrame([
    {"t":"t-2", **y2.to_dict()},
    {"t":"t-1", **y1.to_dict()},
    {"t":"t0",  **y0.to_dict()},
])

# 指標
out["op_margin"]    = out["op_profit"] / out["sales"]
out["roa"]          = out["net_income"] / out["total_assets"]
out["equity_ratio"] = out["equity"] / out["total_assets"]

# 見やすく（億円・%）列も追加
out["sales_oku"] = out["sales"] / 1e8
out["op_margin_pct"] = out["op_margin"] * 100
out["roa_pct"] = out["roa"] * 100
out["equity_ratio_pct"] = out["equity_ratio"] * 100

out.to_csv("work/olc/timeseries_3y.csv", index=False, encoding="utf-8-sig")

print(out[["t","sales_oku","op_margin_pct","roa_pct","equity_ratio_pct"]])
print("\nSaved: work/olc/timeseries_3y.csv")
print("new_doc (t0,t-1):", new_doc)
print("prev_doc (t-2):", prev_doc)
