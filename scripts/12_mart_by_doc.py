import duckdb
import pandas as pd

con = duckdb.connect("work/olc/olc.duckdb")

# doc_idごとに主要科目を作る（当期/前期）
df = con.execute("""
WITH facts AS (
  SELECT doc_id, period, item, value_num FROM bs
  UNION ALL
  SELECT doc_id, period, item, value_num FROM pl
),
mart AS (
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
)
SELECT * FROM mart
ORDER BY doc_id, CASE period
  WHEN 'Prior2Year' THEN 1
  WHEN 'Prior1Year' THEN 2
  WHEN 'CurrentYear' THEN 3
  ELSE 9 END
""").df()

print(df)

# ここでは「時系列化の材料」をCSVにして目視できるように保存
df.to_csv("work/olc/mart_by_doc_period.csv", index=False, encoding="utf-8-sig")
print("\nSaved: work/olc/mart_by_doc_period.csv")
