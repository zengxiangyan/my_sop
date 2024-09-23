#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import column

from config import DB_URL_DK_VM

engine = create_engine(DB_URL_DK_VM)

engine.execute("DROP TABLE IF EXISTS category;")
engine.execute(
    """
    CREATE TABLE category (
    `L1NO` INTEGER NOT NULL,
    `L1EN` VARCHAR(50) NOT NULL,
    `L2NO` INTEGER NOT NULL,
    `L2EN` VARCHAR(50) NOT NULL,
    `L3NO` INTEGER NOT NULL,
    `L3EN` VARCHAR(50) NOT NULL,
    `L3CN` VARCHAR(50) NOT NULL,
    `no` double NOT NULL UNIQUE
    );
    """
)
df = pd.read_excel(
    "rules.xlsx",
    sheet_name="category.lib",
    usecols=["L1NO", "L1EN", "L2NO", "L2EN", "L3NO", "L3EN", "no", "分类结果"],
)
df.rename(columns={"分类结果": "L3CN"}, inplace=True)
df.drop_duplicates(inplace=True)
print(df)
df.to_sql("category", engine, if_exists="append", index=False)
