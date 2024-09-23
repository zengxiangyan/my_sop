
# %%
import pandas as pd

df = pd.read_excel("品牌等级待匹配-NINT0709V3.xlsx", sheet_name="TTL")

# %%
df["Brand"]
# %%
df2 = df["Brand"].str.split("/", expand=True)
# %%
df2.to_excel("brandsplit.xlsx")
# %%
