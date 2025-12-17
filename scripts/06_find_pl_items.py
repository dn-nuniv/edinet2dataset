import duckdb
con = duckdb.connect("work/olc/olc.duckdb")

print("sales candidates:")
print(con.execute("SELECT DISTINCT item FROM pl WHERE item LIKE '%売上%' ORDER BY item LIMIT 50").fetchall())

print("\nop candidates:")
print(con.execute("SELECT DISTINCT item FROM pl WHERE item LIKE '%営業%' ORDER BY item LIMIT 50").fetchall())
