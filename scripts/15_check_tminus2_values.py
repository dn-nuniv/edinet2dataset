import duckdb
con = duckdb.connect("work/olc/olc.duckdb")

doc = "S100R8C8"
period = "Prior1Year"   # ←これが t-2

print("PL values for", doc, period)
print(con.execute(f"""
SELECT item, value_num
FROM pl
WHERE doc_id='{doc}' AND period='{period}'
  AND item IN ('売上高','営業利益','当期利益','売上原価','売上総利益又は売上総損失（△)')
ORDER BY item
""").df())

print("\nBS values for", doc, period)
print(con.execute(f"""
SELECT item, value_num
FROM bs
WHERE doc_id='{doc}' AND period='{period}'
  AND item IN ('総資産','純資産','流動資産','固定資産')
ORDER BY item
""").df())
