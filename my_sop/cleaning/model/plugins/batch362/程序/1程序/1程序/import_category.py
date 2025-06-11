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
    connection.execute(text("DROP TABLE IF EXISTS category;"))
    connection.execute(
        text("""
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
        """)
    )
    df = pd.read_excel(sys.path[0] +
        "rules/rules.xlsx",
        sheet_name="category.lib",
        usecols=["L1NO", "L1EN", "L2NO", "L2EN", "L3NO", "L3EN", "no", "分类结果"],
    )
    print(df)
    df.rename(columns={"分类结果": "L3CN"}, inplace=True)
    df.drop_duplicates(inplace=True)
    print(df)
    df.to_sql("category", engine, if_exists="append", index=False)
