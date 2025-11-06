import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
from tqdm import tqdm

def get_csv_from_url(URL):
    soup = BeautifulSoup(requests.get(URL).text, features="html.parser")
    table = soup.table

    links = table.find_all("a")
    online_file_names = []
    file_path = './data/'
    downloaded_files = os.listdir(file_path)

    for l in links:
        file_name = l.get('href')
        if "scribus" in file_name:
            if file_name not in downloaded_files:
                try : 
                    response = requests.get(URL + file_name)
                    response.raise_for_status()

                    total_size = int(response.headers.get('content-length', 0))

                    with open(file_path + file_name, 'wb') as file, \
                        tqdm(desc=file_name, total=total_size, unit='iB', unit_scale=True) as progress_bar:
                        for chunk in response.iter_content(chunk_size=8192):
                            size = file.write(chunk)
                            progress_bar.update(size)
                except requests.exceptions.RequestException as e:
                    print(f"Error when downloading file {file_name} : {e}")
                    

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


get_csv_from_url('http://teachingse.hevs.ch/csvFiles/')
