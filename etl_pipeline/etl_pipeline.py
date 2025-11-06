import pandas as pd
import requests
from bs4 import BeautifulSoup
import re


def get_csv_from_url(URL):
    soup = BeautifulSoup(requests.get(URL).text)
    table = soup.table

    links = table.find_all("a")
    file_names = []

    for l in links:
        file_name = l.get('href')
        if "scribus" in file_name:
            file_names.append(file_name)


def get_data_from_file(file_path):
    data = pd.read_csv(file_path)
    print(data.head())
    print(data.info())
    return data


def clean_data(data):
    data = data.dropna()

    # Clean status, resolution, category, severity, priority
    column_to_lower = [ 'Priority', 'Severity', 'Reproducibility', 'Category', 'Status', 'Resolution' ]
    lowered_columns = data[column_to_lower].apply(lambda x: x.str.lower())
    data.loc[:, column_to_lower] = lowered_columns

    # Check datetime format
    date_columns = ['Date Submitted', 'Updated']
    data.loc[:, date_columns] = data[date_columns].apply(pd.to_datetime)

    return data


df = get_data_from_file('data/scribus-dump-2025-11-03.csv')
cleaned_df = clean_data(df)
get_csv_from_url('http://teachingse.hevs.ch/csvFiles/')
