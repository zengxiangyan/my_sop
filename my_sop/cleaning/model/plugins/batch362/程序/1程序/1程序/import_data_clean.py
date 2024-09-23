#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import column

from config import DB_URL_WQ

engine = create_engine(DB_URL_WQ)

engine.execute("DROP TABLE IF EXISTS makeupall;")
engine.execute(
    """
    CREATE TABLE makeupall (
    `platform` VARCHAR(100) NOT NULL,
    `newno` int NOT NULL UNIQUE,
    `no` int NOT NULL,
    `time` VARCHAR(100) NOT NULL,
    `c4` VARCHAR(100),
    `name` VARCHAR(2000),
    `brand` VARCHAR(2000),
    `url` VARCHAR(2000) NOT NULL,
    `shopname` VARCHAR(100),
    `unitold` int NOT NULL,
    `priceold` float NOT NULL,
    `salesold` float NOT NULL,
    `unit` int NOT NULL,
    `price` float NOT NULL,
    `sales` float NOT NULL,
    `unitlive` int NOT NULL,
    `pricelive` float NOT NULL,
    `c6` VARCHAR(100),
    `Category` VARCHAR(100),
    `SubCategory` VARCHAR(100),
    `SubCategorySegment` VARCHAR(100),
    `BrandName` VARCHAR(100),
    `BrandCN` VARCHAR(100),
    `BrandEN` VARCHAR(100),
    `User` VARCHAR(100),
    `ShopType1` VARCHAR(100),
    `ShopType2` VARCHAR(100),
    `Manufacturer` VARCHAR(100),
    `Division` VARCHAR(100),
    `Selectivity` VARCHAR(100),
    `BrandLRL` VARCHAR(100)
    );
    """
)
df = pd.read_excel(
    "alldata0723.xlsx",
    sheet_name="Sheet1",
)
df.drop_duplicates(inplace=True)
print(df)
engine = create_engine(DB_URL_WQ, pool_recycle=3600, pool_size=5)
df.to_sql("makeupall", engine, if_exists="append", index=False, chunksize=100)