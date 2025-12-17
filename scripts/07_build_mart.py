import duckdb

con = duckdb.connect("work/olc/olc.duckdb")

# BSとPLを結合して「材料（facts）」を作る
con.execute("""
CREATE OR REPLACE VIEW facts AS
SELECT doc_id, period, item, value_num FROM bs
UNION ALL
SELECT doc_id, period, item, value_num FROM pl
""")

# 会社×期間の成績表（1行にまとめる）
con.execute("""
CREATE OR REPLACE TABLE mart_fin AS
SELECT
  doc_id,
  period,
  MAX(CASE WHEN item='売上高'   THEN value_num END) AS sales,
  MAX(CASE WHEN item='営業利益' THEN value_num END) AS op_profit,
  MAX(CASE WHEN item='当期利益' THEN value_num END) AS net_income,
  MAX(CASE WHEN item='総資産'   THEN value_num END) AS total_assets,
  MAX(CASE WHEN item='純資産'   THEN value_num END) AS equity
FROM facts
GROUP BY doc_id, period
""")

df = con.execute("""
SELECT
  period,
  sales,
  op_profit,
  net_income,
  total_assets,
  equity,
  op_profit / NULLIF(sales, 0)        AS op_margin,
  net_income / NULLIF(total_assets,0) AS roa,
  equity / NULLIF(total_assets,0)     AS equity_ratio
FROM mart_fin
ORDER BY CASE period
  WHEN 'Prior2Year' THEN 1
  WHEN 'Prior1Year' THEN 2
  WHEN 'CurrentYear' THEN 3
  ELSE 9 END
""").df()

print(df)

df.to_csv("work/olc/mart_fin.csv", index=False, encoding="utf-8-sig")
print("\nSaved: work/olc/mart_fin.csv")
