"""Pipeline ETL e-commerce directo en Python (pandas y polars).
Mide: wall total, wall por etapa, peak RSS.
Corre en el scheduler container donde hay el dataset."""
import time, os, sys, resource, json

BASE = "/opt/airflow/bench_data"

def peak_rss_mb():
    # Linux: ru_maxrss is in KB
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0

def run_pandas():
    import pandas as pd
    OUT = "/opt/airflow/bench_out_direct_pandas"
    os.makedirs(OUT, exist_ok=True)
    results = {}

    t0 = time.time()
    df = pd.read_parquet(f"{BASE}/transactions.parquet")
    df = df[df["monto"] > 0]
    df = df[df["order_id"].notna()]
    df.to_parquet(f"{OUT}/validas.parquet", index=False)
    t1 = time.time()
    results["validar"] = round(t1 - t0, 2)
    print(f"pandas.validar: {t1-t0:.2f}s rows={len(df)}")

    t0 = time.time()
    df = pd.read_parquet(f"{OUT}/validas.parquet")
    df["categoria"] = df["categoria"].str.lower()
    df.to_parquet(f"{OUT}/limpias.parquet", index=False)
    t1 = time.time()
    results["limpiar"] = round(t1 - t0, 2)
    print(f"pandas.limpiar: {t1-t0:.2f}s rows={len(df)}")

    t0 = time.time()
    df = pd.read_parquet(f"{OUT}/limpias.parquet")
    cust = pd.read_parquet(f"{BASE}/customers.parquet")
    df = df.merge(cust, on="customer_id", how="inner")
    df.to_parquet(f"{OUT}/con_cliente.parquet", index=False)
    t1 = time.time()
    results["join_customers"] = round(t1 - t0, 2)
    print(f"pandas.join_customers: {t1-t0:.2f}s rows={len(df)}")

    t0 = time.time()
    df = pd.read_parquet(f"{OUT}/con_cliente.parquet")
    prod = pd.read_parquet(f"{BASE}/products.parquet")
    df = df.merge(prod, on="product_id", how="inner", suffixes=("_x","_y"))
    if "categoria_x" in df.columns and "categoria_y" in df.columns:
        df["categoria"] = df["categoria_x"].fillna(df["categoria_y"])
        df = df.drop(columns=["categoria_x","categoria_y"])
    df.to_parquet(f"{OUT}/enriquecida.parquet", index=False)
    t1 = time.time()
    results["join_products"] = round(t1 - t0, 2)
    print(f"pandas.join_products: {t1-t0:.2f}s rows={len(df)}")

    t0 = time.time()
    df = pd.read_parquet(f"{OUT}/enriquecida.parquet")
    df["dia"] = pd.to_datetime(df["ts"]).dt.date
    agg = (df.groupby(["dia", "categoria"], as_index=False)
           .agg(total=("monto", "sum"), n_trans=("id", "count"))
           .sort_values("dia").reset_index(drop=True))
    agg.to_parquet(f"{OUT}/rollups.parquet", index=False)
    t1 = time.time()
    results["rollups"] = round(t1 - t0, 2)
    print(f"pandas.rollups: {t1-t0:.2f}s rows={len(agg)}")

    t0 = time.time()
    df = pd.read_parquet(f"{OUT}/rollups.parquet")
    df.to_csv(f"{OUT}/rollups.csv", index=False)
    t1 = time.time()
    results["export"] = round(t1 - t0, 2)
    print(f"pandas.export: {t1-t0:.2f}s rows={len(df)}")

    peak = peak_rss_mb()
    total = sum(results.values())
    print(f"\nPANDAS TOTAL: {total:.2f}s  PEAK_RSS: {peak:.0f}MB")
    print(f"PANDAS_RESULTS = {json.dumps(results)}")
    return total, peak, results

def run_polars():
    import polars as pl
    OUT = "/opt/airflow/bench_out_direct_polars"
    os.makedirs(OUT, exist_ok=True)
    results = {}

    t0 = time.time()
    df = pl.read_parquet(f"{BASE}/transactions.parquet")
    df = df.filter((pl.col("monto") > 0) & pl.col("order_id").is_not_null())
    df.write_parquet(f"{OUT}/validas.parquet")
    t1 = time.time()
    results["validar"] = round(t1 - t0, 2)
    print(f"polars.validar: {t1-t0:.2f}s rows={df.height}")

    t0 = time.time()
    df = pl.read_parquet(f"{OUT}/validas.parquet")
    df = df.with_columns(pl.col("categoria").str.to_lowercase())
    df.write_parquet(f"{OUT}/limpias.parquet")
    t1 = time.time()
    results["limpiar"] = round(t1 - t0, 2)
    print(f"polars.limpiar: {t1-t0:.2f}s rows={df.height}")

    t0 = time.time()
    df = pl.read_parquet(f"{OUT}/limpias.parquet")
    cust = pl.read_parquet(f"{BASE}/customers.parquet")
    df = df.join(cust, on="customer_id", how="inner")
    df.write_parquet(f"{OUT}/con_cliente.parquet")
    t1 = time.time()
    results["join_customers"] = round(t1 - t0, 2)
    print(f"polars.join_customers: {t1-t0:.2f}s rows={df.height}")

    t0 = time.time()
    df = pl.read_parquet(f"{OUT}/con_cliente.parquet")
    prod = pl.read_parquet(f"{BASE}/products.parquet")
    df = df.join(prod, on="product_id", how="inner", coalesce=True)
    df.write_parquet(f"{OUT}/enriquecida.parquet")
    t1 = time.time()
    results["join_products"] = round(t1 - t0, 2)
    print(f"polars.join_products: {t1-t0:.2f}s rows={df.height}")

    t0 = time.time()
    df = pl.read_parquet(f"{OUT}/enriquecida.parquet")
    agg = (df.with_columns(pl.col("ts").dt.date().alias("dia"))
           .group_by(["dia", "categoria"])
           .agg(pl.col("monto").sum().alias("total"),
                pl.col("id").count().alias("n_trans"))
           .sort("dia"))
    agg.write_parquet(f"{OUT}/rollups.parquet")
    t1 = time.time()
    results["rollups"] = round(t1 - t0, 2)
    print(f"polars.rollups: {t1-t0:.2f}s rows={agg.height}")

    t0 = time.time()
    df = pl.read_parquet(f"{OUT}/rollups.parquet")
    df.write_csv(f"{OUT}/rollups.csv")
    t1 = time.time()
    results["export"] = round(t1 - t0, 2)
    print(f"polars.export: {t1-t0:.2f}s rows={df.height}")

    peak = peak_rss_mb()
    total = sum(results.values())
    print(f"\nPOLARS TOTAL: {total:.2f}s  PEAK_RSS: {peak:.0f}MB")
    print(f"POLARS_RESULTS = {json.dumps(results)}")
    return total, peak, results

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "both"
    if mode in ("pandas", "both"):
        run_pandas()
    if mode in ("polars", "both"):
        run_polars()