"""Pipeline ETL e-commerce directo (sin Airflow), pandas.
Mismo grafo: validar -> limpiar -> join_cust -> join_prod -> rollups -> export."""
import time, os, resource

BASE = "/opt/airflow/bench_data"
OUT = "/opt/airflow/bench_out_direct_pandas"
os.makedirs(OUT, exist_ok=True)

import pandas as pd

results = {}

t0 = time.time()
df = pd.read_parquet(f"{BASE}/transactions.parquet")
df = df[df["monto"] > 0]
df = df[df["order_id"].notna()]
df.to_parquet(f"{OUT}/validas.parquet", index=False)
t1 = time.time()
results["validar"] = (t1 - t0, len(df))
print(f"validar: {t1-t0:.2f}s rows={len(df)}")

t0 = time.time()
df = pd.read_parquet(f"{OUT}/validas.parquet")
df["categoria"] = df["categoria"].str.lower()
df.to_parquet(f"{OUT}/limpias.parquet", index=False)
t1 = time.time()
results["limpiar"] = (t1 - t0, len(df))
print(f"limpiar: {t1-t0:.2f}s rows={len(df)}")

t0 = time.time()
df = pd.read_parquet(f"{OUT}/limpias.parquet")
cust = pd.read_parquet(f"{BASE}/customers.parquet")
df = df.merge(cust, on="customer_id", how="left")
df.to_parquet(f"{OUT}/con_cliente.parquet", index=False)
t1 = time.time()
results["join_customers"] = (t1 - t0, len(df))
print(f"join_customers: {t1-t0:.2f}s rows={len(df)}")

t0 = time.time()
df = pd.read_parquet(f"{OUT}/con_cliente.parquet")
prod = pd.read_parquet(f"{BASE}/products.parquet")
df = df.merge(prod, on="product_id", how="left")
df.to_parquet(f"{OUT}/enriquecida.parquet", index=False)
t1 = time.time()
results["join_products"] = (t1 - t0, len(df))
print(f"join_products: {t1-t0:.2f}s rows={len(df)}")

t0 = time.time()
df = pd.read_parquet(f"{OUT}/enriquecida.parquet")
df["dia"] = pd.to_datetime(df["ts"]).dt.date
agg = (df.groupby(["dia", "categoria"], as_index=False)
       .agg(total=("monto", "sum"), n_trans=("id", "count"))
       .sort_values("dia").reset_index(drop=True))
agg.to_parquet(f"{OUT}/rollups.parquet", index=False)
t1 = time.time()
results["rollups"] = (t1 - t0, len(agg))
print(f"rollups: {t1-t0:.2f}s rows={len(agg)}")

t0 = time.time()
df = pd.read_parquet(f"{OUT}/rollups.parquet")
df.to_csv(f"{OUT}/rollups.csv", index=False)
t1 = time.time()
results["export"] = (t1 - t0, len(df))
print(f"export: {t1-t0:.2f}s rows={len(df)}")

peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
print(f"\nTOTAL_PANDAS_S = {sum(v[0] for v in results.values()):.2f}")
print(f"PEAK_RSS_MB = {peak_kb/1024:.0f}")