#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import column

from config import DB_URL2

engine = create_engine(DB_URL2)

engine.execute("DROP TABLE IF EXISTS makeup;")
engine.execute(
    """
    CREATE TABLE makeup (
    `no`      INTEGER,
    `time`    VARCHAR(50),
    `platform`    VARCHAR(50),
    `c1`      VARCHAR(50),
    `c2`      VARCHAR(50),
    `c3`      VARCHAR(50),
    `c4`     VARCHAR(50),
    `c5`      VARCHAR(50),
    `rank`    INTEGER,
    `name`    VARCHAR(255),
    `url`     VARCHAR(255),
    `Category`  VARCHAR(50),
    `Sub-Category`  VARCHAR(50),
    `Sub-Category-Segment`  VARCHAR(50),
    `BrandNew`  VARCHAR(50),
    `brand`   VARCHAR(50),
    `shop`    VARCHAR(50),
    `volume`  INTEGER,
    `price`   FLOAT,
    `top_90`  VARCHAR(50),
    `before_have` VARCHAR(50)
    );
    """
)
df = pd.read_csv(
    "1-makeup-90.csv", 
    usecols = ["no", "time", "platform", "c1", "c2", "c3", "c4", "c5", "rank", "name", "url", 
    "Category", "Sub-Category", "Sub-Category-Segment", "BrandNew", 
    "brand", "shop", "volume", "price", "top_90", "before_have"],
    sep="\t",
    encoding="gb18030",
)

print(df)
df.to_sql("makeup", engine, if_exists="append", index=False)
