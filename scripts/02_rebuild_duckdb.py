import duckdb

con = duckdb.connect("work/olc/olc.duckdb")

con.execute("CREATE OR REPLACE TABLE bs AS SELECT * FROM read_parquet('work/olc/parsed/bs.parquet')")
con.execute("CREATE OR REPLACE TABLE pl AS SELECT * FROM read_parquet('work/olc/parsed/pl.parquet')")

print("bs rows:", con.execute("SELECT COUNT(*) FROM bs").fetchone()[0])
print("pl rows:", con.execute("SELECT COUNT(*) FROM pl").fetchone()[0])

print("pl sales candidates:",
      con.execute("SELECT DISTINCT item FROM pl WHERE item LIKE '%売上%' ORDER BY item").fetchall())
print("pl op candidates:",
      con.execute("SELECT DISTINCT item FROM pl WHERE item LIKE '%営業%' ORDER BY item").fetchall())
