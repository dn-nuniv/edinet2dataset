import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("work/olc/timeseries_3y.csv")

plt.figure()
plt.plot(df["t"], df["sales_oku"], marker="o")
plt.title("Sales (OLC) [oku yen] (3y)")
plt.tight_layout()
plt.savefig("work/olc/sales_3y.png")
print("Saved: work/olc/sales_3y.png")

plt.figure()
plt.plot(df["t"], df["op_margin_pct"], marker="o")
plt.title("Operating Margin (OLC) [%] (3y)")
plt.tight_layout()
plt.savefig("work/olc/op_margin_3y.png")
print("Saved: work/olc/op_margin_3y.png")
