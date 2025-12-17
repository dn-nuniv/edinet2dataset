import duckdb
con = duckdb.connect("work/olc/olc.duckdb")

doc = "S100R8C8"

print(con.execute(f"""
SELECT period,
  MAX(CASE WHEN item='売上高' THEN value_num END) AS sales,
  MAX(CASE WHEN item='営業利益' THEN value_num END) AS op,
  MAX(CASE WHEN item='当期利益' THEN value_num END) AS ni
FROM pl
WHERE doc_id='{doc}'
GROUP BY period
ORDER BY CASE period WHEN 'Prior2Year' THEN 1 WHEN 'Prior1Year' THEN 2 WHEN 'CurrentYear' THEN 3 ELSE 9 END
""").df())
