import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("work/olc/mart_fin.csv")
order = {"Prior2Year":0, "Prior1Year":1, "CurrentYear":2}
df["ord"] = df["period"].map(order)
df = df.sort_values("ord")

# 売上：億円
df["sales_oku"] = df["sales"] / 1e8

plt.figure()
plt.plot(df["period"], df["sales_oku"], marker="o")
plt.title("Sales (OLC) [oku yen]")
plt.tight_layout()
plt.savefig("work/olc/sales_oku.png")
print("Saved: work/olc/sales_oku.png")

# 営業利益率：%
plt.figure()
plt.plot(df["period"], df["op_margin"] * 100, marker="o")
plt.title("Operating Margin (OLC) [%]")
plt.tight_layout()
plt.savefig("work/olc/op_margin_pct.png")
print("Saved: work/olc/op_margin_pct.png")
