# Creating features for pollutants and weather data.
# Dataframe contains daily means of traffic and weather variables but:
# - max O3 concentration of the current day
# - mean of PM10 concentration of the current day

import pandas as pd

def add_previous_day(df):
    """
    This function returns the value of concentration for each station for the previous day.
    
    Args:
        df (pd.DataFrame): contains daily data of air quality, traffic and weather.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df["previous_day"] = df.groupby("code_site")["concentration"].shift(1)

    return df

def add_previous_2w_mean(df):
    """
    This function calculates the mean of max daily concentration for each station for the previous 14 days.
    
    Args:
        df (pd.DataFrame): contains daily data of air quality, traffic and weather.
    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["code_site", "date"])
    
    # Moving average on past 14 days, shifted by 1 day to avoid data leakage
    df["previous_2w_mean"] = (
        df.groupby("code_site")["concentration"]
        .rolling(window=14, min_periods=1)
        .mean()
        .shift(1)
        .reset_index(level=0, drop=True)
    )
    
    return df

def add_weather(df):
    """
    This function takes the next day's weather data for each station. This is a leakagebut it is
    useful to see how much the weather data can help in predicting the concentration of pollutants.

    Args:
        df (pd.DataFrame): contains daily data of air quality, traffic and weather.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["code_site", "date"])

    weather_columns = ["RR", "TNTXM", "FFM"]
    
    for col in weather_columns:
        df[f"next_{col}"] = df.groupby("code_site")[col].shift(-1)
    
    return df

'''
def add_previous_year_month_mean(df):
    """
    This function calculates the mean of concentration for each station for the previous year and month.
    
    Args:
        df (pd.DataFrame): contains data of air quality, one row per hour and station.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])

    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

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
'''


'''
def add_previous_day_hour(df):
    """
    This function adds a feature for the concentration at the same hour of the previous day for each station.
    
    Args:
        df (pd.DataFrame): contains data of air quality, one row per hour and station.

    Returns:
        pd.DataFrame: A DataFrame with the new feature added.
    """

    df = df.copy()
    df = df.sort_values(["code_site", "date"])
    df["date"] = pd.to_datetime(df["date"])

    df["previous_day_same_hour"] = (
        df.groupby("code_site")["concentration"].shift(1)
    )

    return df
'''