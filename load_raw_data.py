import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import sys

# =========================
# DATABASE CONNECTIE
# =========================
try:
    conn = psycopg2.connect(
        host="localhost",
        database="energy_raw",
        user="energy_user",
        password="energy_pass",
        port=5432
    )
    cur = conn.cursor()
    print("✅ Connected to database")
except Exception as e:
    print(f"❌ Database connectie mislukt: {e}")
    sys.exit()

# =========================
# FUNCTIES
# =========================

def read_csv_clean(path):
    """Leest CSV in en verwijdert witruimte uit kolomnamen."""
    print(f"⌛ Inlezen van {path}...")
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df

def convert_kw_to_w(df, year, time_col="Time"):
    """Zet kW om naar W voor data vóór 6 juni."""
    if time_col not in df.columns:
        return df # Overslaan als Time ontbreekt

    df[time_col] = pd.to_datetime(df[time_col])
    cutoff = pd.Timestamp(f"{year}-06-06")

    # Zoek numerieke kolommen (behalve Year/Apartment/Tariff)
    exclude = ["Year", "Apartment", "Tariff"]
    numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns if c not in exclude]
    
    mask = df[time_col] < cutoff
    df.loc[mask, numeric_cols] = df.loc[mask, numeric_cols] * 1000
    return df

# =========================
# 1. DISTRICT DATA
# =========================
try:
    district_merged = read_csv_clean("district_merged.csv")
    district_merged = convert_kw_to_w(district_merged, year=2021)

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

    district_rows = [
        (
            r.Time, r.Warmtenet, r.Warmtepomp, r.Waterzuivering, 
            r.Vacuum, r.Laadpalen, r.Overig, r.Total, 
            getattr(r, 'Total_calc', 0), r.Year
        )
        for r in district_merged.itertuples(index=False)
    ]

    execute_values(cur, """
        INSERT INTO district_raw (Time, Warmtenet, Warmtepomp, Waterzuivering, Vacuum, Laadpalen, Overig, Total, Total_calc, Year)
        VALUES %s
    """, district_rows)
    conn.commit()
    print("✅ District data opgeslagen")

except Exception as e:
    print(f"❌ Fout bij District data: {e}")

# =========================
# 2. PRIVATE UNITS DATA
# =========================
try:
    private_merged = read_csv_clean("private_units_merged.csv")
    
    # FIX voor de KeyError: we kijken welke naam de kolom heeft
    # De kolom in de CSV is waarschijnlijk 'Power_kW' of 'Power'
    possible_power_cols = ['Power_kW', 'Power_W', 'Power', 'Power_KW']
    found_power_col = next((c for c in possible_power_cols if c in private_merged.columns), None)

    if not found_power_col:
        raise KeyError(f"Power kolom niet gevonden! Beschikbaar: {private_merged.columns.tolist()}")

    private_merged = convert_kw_to_w(private_merged, year=2021)

    cur.execute("DROP TABLE IF EXISTS private_units_raw;")
    cur.execute("""
    CREATE TABLE private_units_raw (
        Time TIMESTAMP,
        Apartment BIGINT,
        Tariff INT,
        Power_W DOUBLE PRECISION,
        Year INT
    );
    """)

    # Gebruik itertuples voor snelheid (126MB verwerking)
    # We gebruiken getattr omdat kolomnamen met hoofdletters/tekens in tuples soms veranderen
    private_rows = [
        (
            pd.to_datetime(r.Time),
            int(r.Apartment),
            int(r.Tariff),
            float(getattr(r, found_power_col)),
            int(r.Year)
        )
        for r in private_merged.itertuples(index=False)
    ]

    execute_values(cur, """
        INSERT INTO private_units_raw (Time, Apartment, Tariff, Power_W, Year)
        VALUES %s
    """, private_rows)
    conn.commit()
    print("✅ Private units data opgeslagen")

except Exception as e:
    print(f"❌ Fout bij Private units data: {e}")

# =========================
# AFSLUITEN
# =========================
cur.close()
conn.close()
print("🚀 RAW DATA STORAGE COMPLETE")