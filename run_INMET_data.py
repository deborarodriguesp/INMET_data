#conda activate precip

import sys
import os
import shutil
import pandas as pd
import numpy as np


def extract_coordinates(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

        station_name = ''
        station_code = ''
        latitude = ''
        longitude = ''

        for line in lines:
            if 'Nome' in line:
                station_name = line.split(':')[1].strip()
            elif 'Codigo Estacao' in line:
                station_code = line.split(':')[1].strip()
            elif 'Latitude' in line:
                latitude = line.split(':')[1].strip()
            elif 'Longitude' in line:
                longitude = line.split(':')[1].strip()

        return station_name, station_code, latitude, longitude
        
def write_coordinates_file(folder_path, output_file):
    with open(output_file, 'w', encoding='utf-8') as output:
        output.write('Station Name\tStation Code\tLatitude\tLongitude\n')

        # Loop through all the files in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):  # Check if the file is a .csv
                file_path = os.path.join(folder_path, filename)
                station_name, station_code, latitude, longitude = extract_coordinates(file_path)

                # Write the extracted information to the output file
                output.write(f'{station_name}\t{station_code}\t{latitude}\t{longitude}\n')

def copy_selected_stations(clipped_stations_file, source_folder, destination_folder):
    # Ler o arquivo das estações selecionadas
    stations_df = pd.read_csv(clipped_stations_file, delimiter='\t')
    
    # Verificar se a pasta de destino existe, senão, criá-la
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)
    
    # Iterar sobre cada estação no arquivo das estações selecionadas
    for _, row in stations_df.iterrows():
        station_id = row['ID']  # Pega o ID da estação
        
        # Procurar o arquivo correspondente no source_folder
        for filename in os.listdir(source_folder):
            if f'dados_{station_id}_' in filename:  # Verifica se o nome do arquivo começa com o ID da estação
                source_file = os.path.join(source_folder, filename)
                destination_file = os.path.join(destination_folder, filename)
                
                # Copiar o arquivo para a pasta de destino
                shutil.copy(source_file, destination_file)
                print(f'{filename} copiado para {destination_folder}')
       
def load_precipitation_data(file_path):
    # Carregar o arquivo de precipitação
    df = pd.read_csv(file_path, delimiter=';', skiprows=10)  # Pular as primeiras linhas de metadados
    
    #Drop the last column because it is empty
    df = df.iloc[:, :-1]
    
    df.columns = ['Date', 'Precipitation']
    df['Precipitation'].replace('null', np.nan, inplace=True)
    # Convert the 'Precipitation' column to numeric values, forcing errors to NaN
    df['Precipitation'] = pd.to_numeric(df['Precipitation'], errors='coerce')
    
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df.set_index('Date', inplace=True)
    
    return df  

def create_dataframe_precipitation(destination_folder):
    # Crie um DataFrame vazio com datas entre 1960 e 2023
    date_range = pd.date_range(start='1960-01-01', end='2022-12-31', freq='D')
    consolidated_df = pd.DataFrame(index=date_range)

    # Loop por cada arquivo de estação
    for filename in os.listdir(destination_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(destination_folder, filename)

            # Carregar os dados da estação
            station_data = load_precipitation_data(file_path)

            # Nome da estação a partir do nome do arquivo
            station_name = filename.split('_')[1]  # Exemplo: 'A001' de 'dados_A001_D_...'

            # Adicionar dados da estação ao DataFrame consolidado
            consolidated_df[station_name] = station_data['Precipitation']
        #consolidated_df.to_csv(df_output_file, sep='\t', index=True, na_rep="NaN")
    
    return consolidated_df

def yearly_precipitation(consolidated_df, yearly_output_file):
    # Resample by month, applying the sum function
    df_monthly = consolidated_df.resample('M').apply(lambda x: np.nan if x.isna().any() else x.sum())
    
    # Resample by year to get yearly sums
    df_yearly_precipitation = df_monthly.resample('Y').apply(lambda x: x.sum() if x.notna().all() else np.nan)
    df_yearly_precipitation.to_csv(yearly_output_file, sep='\t', index=True, na_rep="NaN")
 
if __name__ == "__main__":
    
    folder_path = 'E:/METEOROLOGIA/Precipitation_scenarious/Annual_SPI/INMET_2024/Convencionais/'
    output_file = 'INMET_station_coordinates.txt'
    
    #write_coordinates_file(folder_path, output_file)

    """ Second step:
    After extracting the coordinates, use QGIS to clip only the stations 
    inside Tocantins Araguaia watershed """

    """Third step: 
    select only the data there is inside TAW """
    clipped_stations_file = 'E:/METEOROLOGIA/Precipitation_scenarious/Annual_SPI/INMET_2024/INMET_statios_taw.txt'
    source_folder = 'E:/METEOROLOGIA/Precipitation_scenarious/Annual_SPI/INMET_2024/Convencionais/'
    destination_folder = 'E:/METEOROLOGIA/Precipitation_scenarious/Annual_SPI/INMET_2024/Selected_Stations_TAW/'

    #copy_selected_stations(clipped_stations_file, source_folder, destination_folder)

    """Fourth step:
    After separeting the stations, time to analyze """
    df_output_file = 'dataframe_precip_INMET.txt'
    df_consolidated = create_dataframe_precipitation(destination_folder)

    yearly_output_file = 'df_yearly_precipitation_INMET.csv'
    yearly_precipitation(df_consolidated,yearly_output_file)