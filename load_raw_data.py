import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

# =========================
# DATABASE CONNECTIE
# =========================

conn = psycopg2.connect(
    host="localhost",
    database="energy_raw",
    user="energy_user",
    password="energy_pass",
    port=5432
)

cur = conn.cursor()
print("Connected to database")

# =========================
# FUNCTIES
# =========================

def read_csv_clean(path):
    """
    Robuust CSV inlezen:
    - Detecteert delimiter
    - Verwijdert BOM
    - Verwijdert spaties in kolomnamen
    """
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df

def convert_kw_to_w(df, year, time_col="Time"):
    """
    Zet kW om naar W voor data vóór 6 juni.
    """
    if time_col not in df.columns:
        raise ValueError(f"Kolom '{time_col}' niet gevonden. Beschikbare kolommen: {df.columns.tolist()}")

    df[time_col] = pd.to_datetime(df[time_col])
    cutoff = pd.Timestamp(f"{year}-06-06")

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    mask = df[time_col] < cutoff
    df.loc[mask, numeric_cols] = df.loc[mask, numeric_cols] * 1000
    return df

# =========================
# DISTRICT DATA
# =========================

district_merged = read_csv_clean("district_merged.csv")
district_merged = convert_kw_to_w(district_merged, year=2021)  # conversie voor alle rijen is veilig

# Check kolommen
print("District kolommen:", district_merged.columns.tolist())

# Tabel aanmaken
cur.execute("DROP TABLE IF EXISTS district_raw;")
cur.execute("""
CREATE TABLE district_raw (
    Time TIMESTAMP,
    Warmtenet DOUBLE PRECISION,
    Warmtepomp DOUBLE PRECISION,
    Waterzuivering DOUBLE PRECISION,
    Vacuum DOUBLE PRECISION,
    Laadpalen DOUBLE PRECISION,
    Overig DOUBLE PRECISION,
    Total DOUBLE PRECISION,
    Total_calc DOUBLE PRECISION,
    Year INT
);
""")
print("District tabel aangemaakt")

# Bulk insert met execute_values
district_rows = [
    (
        row["Time"],
        row["Warmtenet"],
        row["Warmtepomp"],
        row["Waterzuivering"],
        row["Vacuum"],
        row["Laadpalen"],
        row["Overig"],
        row["Total"],
        row.get("Total_calc", 0),  # fallback als ontbreekt
        row["Year"]
    )
    for _, row in district_merged.iterrows()
]

execute_values(
    cur,
    """
    INSERT INTO district_raw
    (Time, Warmtenet, Warmtepomp, Waterzuivering, Vacuum, Laadpalen, Overig, Total, Total_calc, Year)
    VALUES %s
    """,
    district_rows
)
conn.commit()
print("District raw data opgeslagen")

# =========================
# PRIVATE UNITS DATA
# =========================

private_merged = read_csv_clean("private_units_merged.csv")
private_merged = convert_kw_to_w(private_merged, year=2021)  # conversie veilig

# Tabel aanmaken
cur.execute("DROP TABLE IF EXISTS private_units_raw;")
cur.execute("""
CREATE TABLE private_units_raw (
    Time TIMESTAMP,
    Apartment BIGINT,
    Tariff INT,
    Power_kW DOUBLE PRECISION,
    Year INT
);
""")
print("Private units tabel aangemaakt")

# Bulk insert
private_rows = [
    (
        row["Time"],
        int(row["Apartment"]),
        int(row["Tariff"]),
        row["Power_W"],  # kolomnaam in CSV
        int(row["Year"])
    )
    for _, row in private_merged.iterrows()
]

execute_values(
    cur,
    """
    INSERT INTO private_units_raw
    (Time, Apartment, Tariff, Power_KW, Year)
    VALUES %s
    """,
    private_rows
)
conn.commit()
print("Private units raw data opgeslagen")

# =========================
# CONNECTIE SLUITEN
# =========================

cur.close()
conn.close()
print("RAW DATA STORAGE COMPLETE 🚀")