#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine,text
from sqlalchemy.sql.expression import column
from config import DB_URL_DY_VM, DB_URL_KW_VM, DB_URL_DK_VM, DB_URL_WQ_VM
engine = create_engine(DB_URL_WQ_VM)
import sys
from os.path import abspath, join, dirname
sys.path.insert(0, join(abspath(dirname(__file__)), '../../../'))
print(sys.path[0])
with engine.connect() as connection:
    connection.execute(text("DROP TABLE IF EXISTS brand;"))

    connection.execute(text(
        """
        CREATE TABLE IF NOT EXISTS brand(
            BrandName VARCHAR(200) NOT NULL UNIQUE COLLATE utf8mb4_bin,
            BrandCN   VARCHAR(200),
            BrandEN   VARCHAR(200),
            Manufacturer VARCHAR(200),
            User VARCHAR(200),
            Selectivity VARCHAR(200)
        );
        """)
    )

    column_mapping = {
        "BrandName": "BrandName",
        "BrandCN": "BrandCN",
        "BrandEN": "BrandEN",
        "Manufacturer": "Manufacturer",
        "User": "User",
        "Selectivity": "Selectivity",
    }

    df = pd.read_excel(sys.path[0] +
        "rules/rules.xlsx",
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
    df = df.drop_duplicates(subset=['BrandName'])
    print(df)
    df.to_sql(
        "brand",
        con=engine,
        if_exists="append",
        index=False,
    )
