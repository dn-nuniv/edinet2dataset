import duckdb
con = duckdb.connect("work/olc/olc.duckdb")

# bs は既にある前提。pl を追加
con.execute("""
CREATE OR REPLACE TABLE pl AS
SELECT * FROM read_parquet('work/olc/parsed/pl.parquet')
""")

print("pl rows:", con.execute("SELECT COUNT(*) FROM pl").fetchone()[0])
print("pl periods:", con.execute("SELECT DISTINCT period FROM pl ORDER BY period").fetchall())
print("pl items sample:", con.execute("SELECT DISTINCT item FROM pl ORDER BY item LIMIT 30").fetchall())
