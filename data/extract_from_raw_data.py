import pandas as pd
import numpy as np
from glob import glob
import os


directory = os.getcwd() + '/Traffic_Flow_Data'
filenames = glob(directory + '/ind*')

df = pd.DataFrame()

def df_from_filenames(filenames):
    """
    Create dataframe with reqd columns from given list of filenames
    """
    
    for i in filenames:
        try:

            data = pd.read_csv(i)
            df2 = data[data['Detector']==1.2].groupby(['Datum', 'Uur']).mean()[['Waarde']].reset_index()
            df2['Datum'] = pd.to_datetime(df2['Datum'], format = "%Y-%m-%d")
            df2 = df2.sort_values(['Datum', 'Uur'])
            df = df.append(df2)
        
        except: 
            
            data = pd.read_csv(i, sep = ';')
            data = data.rename(columns = {"Long":"longitude", "Lat":"latitude"})
            data['longitude'] = data['longitude'].str.replace(',', '.').astype('float')
            data['latitude'] = data['latitude'].str.replace(',', '.').astype('float')
            df2 = data[data['Detector']==1.2].groupby(['Datum', 'Uur']).mean()[['Waarde']].reset_index()
            lat, lon = data['latitude'][0], data['longitude'][0]

            try:
                df2['Datum'] = pd.to_datetime(df2['Datum'], format = "%d-%m-%Y")
            except:
                df2['Datum'] = pd.to_datetime(df2['Datum'], format = "%Y-%m-%d")
            
            df2 = df2.sort_values(['Datum', 'Uur'])
            df = df.append(df2)

    
    df['longitude'] = lon
    df['latitude'] = lat

    return df

