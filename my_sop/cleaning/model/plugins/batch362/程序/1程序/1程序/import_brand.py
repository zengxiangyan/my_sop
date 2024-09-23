#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import column
from config import DB_URL_DY_VM, DB_URL_KW_VM, DB_URL_DK_VM, DB_URL_WQ_VM

engine = create_engine(DB_URL_WQ_VM)

engine.execute("DROP TABLE IF EXISTS brand;")
engine.execute(
    """
    CREATE TABLE IF NOT EXISTS brand(
        BrandName VARCHAR(200) NOT NULL UNIQUE,
        BrandCN   VARCHAR(200),
        BrandEN   VARCHAR(200),
        Manufacturer VARCHAR(200),
        User VARCHAR(200),
        Selectivity VARCHAR(200)
    );
    """
)

column_mapping = {
    "BrandName": "BrandName",
    "BrandCN": "BrandCN",
    "BrandEN": "BrandEN",
    "Manufacturer": "Manufacturer",
    "User": "User",
    "Selectivity": "Selectivity",
}

df = pd.read_excel(
    "rules.xlsx",
    sheet_name="brand.lib",
    dtype=str,
    usecols=column_mapping.keys(),
    keep_default_na=False,
)
df.rename(
    columns=column_mapping,
    inplace=True,
)
df["BrandName"] = df["BrandName"].str.strip()
df.drop_duplicates(subset="BrandName", inplace=True)
print(df)
df.to_sql(
    "brand",
    con=engine,
    if_exists="append",
    index=False,
)
