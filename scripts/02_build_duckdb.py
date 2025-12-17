import duckdb

con = duckdb.connect("work/olc/olc.duckdb")
con.execute("""
CREATE OR REPLACE TABLE bs AS
SELECT * FROM read_parquet('work/olc/parsed/*.parquet')
""")

print("rows:", con.execute("SELECT COUNT(*) FROM bs").fetchone()[0])
print(con.execute("""
SELECT period, item, value_num
FROM bs
WHERE item LIKE '%現金%'
LIMIT 10
""").fetchall())
