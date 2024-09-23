#!/usr/bin/env python3

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql.expression import column

from config import DB_URL3

engine = create_engine(DB_URL3)

engine.execute("DROP TABLE IF EXISTS makeup;")
engine.execute(
    """
    CREATE TABLE makeup (
    `time`    VARCHAR(50),
    `c1`      VARCHAR(50),
    `c2`      VARCHAR(50),
    `name`    VARCHAR(255),
    `shop`    VARCHAR(50),
    `brand`   VARCHAR(50),
    `volume`  INTEGER,
    `sales`   FLOAT,
    `price`   FLOAT,
    `url`     VARCHAR(255),
    );
    """
)
df = pd.read_excel(
    "kw.csv", 
    usecols = ["time", "c1", "c2", "name", "shop", "brand", "volume", "sales", "price", "url"],
    sep="\t",
    encoding="gb18030",
    sheet_name="5",
)

print(df)
df.to_sql("makeup", engine, if_exists="append", index=False)
