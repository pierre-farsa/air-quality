# Creating features for pollutant O3

import pandas as pd


def add_previous_year_month_mean(df):
    """
    This function calculates the mean of concentration for each station for the previous year and month.
    
    Args:
        df (pd.DataFrame): contains data of air quality.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()
    df["date_fin"] = pd.to_datetime(df["date_fin"])

    df["year"] = df["date_fin"].dt.year
    df["month"] = df["date_fin"].dt.month

    monthly_mean = (
        df.groupby(["year", "month", "code_site"])["concentration"]
        .mean()
        .reset_index()
        .rename(columns={"concentration": "monthly_mean"})
    )

    monthly_mean["year"] += 1

    df = df.merge(
        monthly_mean,
        on=["year", "month", "code_site"],
        how="left",
    )

    df.drop(columns=["year", "month"], inplace=True)

    return df



def add_previous_day_mean(df):
    """
    This function calculates the mean of concentration for each station for the previous day.
    
    Args:
        df (pd.DataFrame): contains data of air quality.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()
    df["date_fin"] = pd.to_datetime(df["date_fin"])

    df["date"] = df["date_fin"].dt.floor("D")

    daily_mean = (
        df.groupby(["date", "code_site"])["concentration"]
        .mean()
        .reset_index()
        .rename(columns={"concentration": "previous_day_mean"})
    )

    daily_mean["date"] = daily_mean["date"] + pd.Timedelta(days=1)

    df = df.merge(
        daily_mean,
        on=["date", "code_site"],
        how="left"
    )

    df.drop(columns=["date"], inplace=True)

    return df



def add_previous_day_hour(df):
    """
    This function adds a feature for the concentration at the same hour of the previous day for each station.
    
    Args:
        df (pd.DataFrame): contains data of air quality.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()
    df = df.sort_values(["code_site", "date_fin"])
    df["date_fin"] = pd.to_datetime(df["date_fin"])

    df["previous_day_same_hour"] = (
        df.groupby("code_site")["concentration"].shift(24)
    )

    return df


def add_previous_2w_mean(df):
    df = df.copy()
    df["date_fin"] = pd.to_datetime(df["date_fin"])
    df["date"] = df["date_fin"].dt.floor("D")

    daily = (
        df.groupby(["code_site", "date"])["concentration"]
        .mean()
        .reset_index()
        .sort_values(["code_site", "date"])
    )

    daily["previous_2w_mean"] = (
        daily.groupby("code_site")["concentration"]
        .rolling(window=14, min_periods=1)
        .mean()
        .shift(1) # exclude the current day from the mean calculation
        .reset_index(level=0, drop=True)
    )

    df = df.merge(
        daily[["code_site", "date", "previous_2w_mean"]],
        on=["code_site", "date"],
        how="left"
    )

    df.drop(columns=["date"], inplace=True)

    return df



def add_weather(df):
    df = df.copy()
    df["date_fin"] = pd.to_datetime(df["date_fin"])
    df["date"] = df["date_fin"].dt.floor("D")

    weather_columns = ["RR", "TNTXM", "FFM"]

    # 1. Une ligne météo par jour
    daily = df.groupby(["code_site", "date"], as_index=False)[weather_columns].first()

    # 2. Décaler la date d'un jour)
    daily["date"] = daily["date"] + pd.Timedelta(days=1)

    # 3. Renommer les colonnes
    daily = daily.rename(columns={col: f"previous_{col}" for col in weather_columns})

    # 4. Merge simple
    df = df.merge(daily, on=["code_site", "date"], how="left")

    df.drop(columns="date", inplace=True)

    return df