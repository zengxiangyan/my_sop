import pandas as pd

df = pd.read_excel("rules.xlsx", sheet_name="brand.lib", dtype=object)

def to_str(s):
    if pd.isna(s):
        return ''
    return str(s).strip()

output=[]
for _, row in df.iterrows():
    brand_name = to_str(row["BrandName"])
    brand_en = to_str(row["BrandEN"])
    brand_zh = to_str(row["BrandCN"])
    if brand_name:
        if brand_en:
            output.append([brand_name, "关键词", brand_en])
        if brand_zh:
            output.append([brand_name, "关键词", brand_zh])

df2 = pd.DataFrame(output)
df2.to_excel("convert_brand.xlsx",sheet_name='brand.cv')