import duckdb

con = duckdb.connect("work/olc/olc.duckdb")

con.execute("""
CREATE OR REPLACE TABLE bs AS
SELECT * FROM read_parquet('work/olc/parsed/bs_*.parquet')
""")

con.execute("""
CREATE OR REPLACE TABLE pl AS
SELECT * FROM read_parquet('work/olc/parsed/pl_*.parquet')
""")

print("bs rows:", con.execute("SELECT COUNT(*) FROM bs").fetchone()[0])
print("pl rows:", con.execute("SELECT COUNT(*) FROM pl").fetchone()[0])

print("pl periods:", con.execute("select period, count(*) from pl group by period order by period").fetchall())
print("bs periods:", con.execute("select period, count(*) from bs group by period order by period").fetchall())
