import duckdb

con = duckdb.connect("work/olc/olc.duckdb")

print("PL periods:", con.execute(
    "select period, count(*) from pl group by period order by period"
).fetchall())

print("BS periods:", con.execute(
    "select period, count(*) from bs group by period order by period"
).fetchall())

print("BS 総資産:", con.execute(
    "select period, value_num from bs where item='総資産' order by period"
).fetchall())

print("BS 純資産:", con.execute(
    "select period, value_num from bs where item='純資産' order by period"
).fetchall())

# Prior2Year で「資産」を含む科目名候補も出す（別名チェック用）
print("Prior2Year 資産候補:", con.execute(
    "select distinct item from bs where period='Prior2Year' and item like '%資産%' order by item"
).fetchall())
