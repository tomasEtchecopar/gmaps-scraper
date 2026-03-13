"""
Filtra negocios sin web y exporta dos CSVs:
  - sin_web.csv  → tu target principal
  - con_web.csv  → descartados o segmento aparte

Uso:
  python filtrar.py pymes_argentina.csv
"""

import sys
import pandas as pd

INPUT = sys.argv[1] if len(sys.argv) > 1 else "pymes_argentina.csv"

df = pd.read_csv(INPUT)

sin_web = df[df["web"].isna() | (df["web"].str.strip() == "")]
con_web = df[~df.index.isin(sin_web.index)]

sin_web.to_csv("sin_web.csv", index=False, encoding="utf-8-sig")
con_web.to_csv("con_web.csv", index=False, encoding="utf-8-sig")

print(f"Total:    {len(df)}")
print(f"Sin web:  {len(sin_web)}  → sin_web.csv")
print(f"Con web:  {len(con_web)}  → con_web.csv")
