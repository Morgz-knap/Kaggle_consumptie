import pandas as pd
import numpy as np

# ==============================
# FILE PATHS (AANPASSEN)
# ==============================
district_2021_path = "2021_ElectricPower_15min.csv"
district_2022_path = "2022_ElectricPower_15min.csv"
private_2021_path = "2021_ElectricPowerPrivateUnits_15min.csv"
private_2022_path = "2022_ElectricPowerPrivateUnits_15min.csv"


# ==============================
# ROBUUST CSV INLEZEN
# ==============================
def read_csv_clean(path):
    """
    Leest CSV robuust in:
    - detecteert ; of ,
    - verwijdert BOM
    - verwijdert spaties in kolomnamen
    """
    df = pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df


# ==============================
# ALLES NAAR kW
# ==============================
def convert_all_to_kw(df, year, time_col="Time"):
    """
    Zet alle energie kolommen naar kW.
    Voor 6 juni staat data al in kW.
    Na 6 juni staat data in W → delen door 1000.
    """
    if time_col not in df.columns:
        raise ValueError(f"Kolom '{time_col}' niet gevonden.")

    df[time_col] = pd.to_datetime(df[time_col])
    cutoff = pd.Timestamp(f"{year}-06-06")

    numeric_cols = df.select_dtypes(include=[np.number]).columns

    mask = df[time_col] >= cutoff

    # W → kW
    df.loc[mask, numeric_cols] = df.loc[mask, numeric_cols] / 1000

    return df


# ==============================
# 1️⃣ DISTRICT DATA
# ==============================
def process_district(path, year):
    df = read_csv_clean(path)

    df = convert_all_to_kw(df, year)

    df["Year"] = year

    components = [
        "Warmtenet",
        "Warmtepomp",
        "Waterzuivering",
        "Vacuum",
        "Laadpalen",
        "Overig"
    ]

    missing = [c for c in components if c not in df.columns]
    if missing:
        raise ValueError(f"Ontbrekende kolommen: {missing}")

    # herbereken total
    df["Total_calc"] = df[components].sum(axis=1)

    return df


district_2021 = process_district(district_2021_path, 2021)
district_2022 = process_district(district_2022_path, 2022)

district_merged = pd.concat([district_2021, district_2022])
district_merged = district_merged.sort_values("Time")

district_merged.to_csv("district_merged.csv", index=False)

print("✔ District data merged.")


# ==============================
# 2️⃣ PRIVATE UNITS
# ==============================
def process_private_2021(path):
    df = read_csv_clean(path)

    # private units zijn altijd W → kW
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols] / 1000

    df["Time"] = pd.to_datetime(df["Time"])

    df_long = df.melt(
        id_vars=["Time"],
        var_name="Apartment",
        value_name="Power_kW"
    )

    df_long["Apartment"] = df_long["Apartment"].astype(int)
    df_long["Tariff"] = 0
    df_long["Year"] = 2021

    return df_long


def process_private_2022(path):
    df = read_csv_clean(path)

    df["Time"] = pd.to_datetime(df["Time"])

    # verwijder duplicates
    df = df.loc[:, ~df.columns.duplicated()]

    # alleen kolommen met x.y structuur
    valid_cols = ["Time"] + [c for c in df.columns if c.count(".") == 1]
    df = df[valid_cols]

    # W → kW
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    df[numeric_cols] = df[numeric_cols] / 1000

    df_long = df.melt(
        id_vars=["Time"],
        var_name="Apartment_Tariff",
        value_name="Power_kW"
    )

    split_cols = df_long["Apartment_Tariff"].str.split(".", expand=True)
    split_cols.columns = ["Apartment", "Tariff"]

    df_long["Apartment"] = split_cols["Apartment"].astype(int)
    df_long["Tariff"] = split_cols["Tariff"].astype(int)
    df_long["Year"] = 2022

    df_long = df_long.drop(columns=["Apartment_Tariff"])

    return df_long


private_2021 = process_private_2021(private_2021_path)
private_2022 = process_private_2022(private_2022_path)

private_merged = pd.concat([private_2021, private_2022])
private_merged = private_merged.sort_values(["Time", "Apartment"])

private_merged.to_csv("private_units_merged.csv", index=False)

print("✔ Private units merged.")
print("FASE 1 COMPLEET 🚀")