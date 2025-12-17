import duckdb

con = duckdb.connect("work/olc/olc.duckdb")

print("tables:", con.execute("SHOW TABLES").fetchall())
print("bs rows:", con.execute("SELECT COUNT(*) FROM bs").fetchone()[0])

print("\nperiods:")
print(con.execute("SELECT DISTINCT period FROM bs ORDER BY period").fetchall())

print("\nitems sample:")
print(con.execute("SELECT DISTINCT item FROM bs ORDER BY item LIMIT 30").fetchall())

print("\nsearch: total assets candidates (資産):")
print(con.execute("SELECT DISTINCT item FROM bs WHERE item LIKE '%資産%' ORDER BY item LIMIT 30").fetchall())
