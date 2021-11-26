import pandas as pd
from pandas.api.types import is_string_dtype


def drop_nan_names(dataframe):
    '''
        Delete any rows which do not have a name 
    '''
    return dataframe[dataframe['name'].notna()]


def split_names(dataframe):
    '''
        Split the name field into first_name, and last_name
    '''
    dataframe[['first_name', 'last_name']] = dataframe['name'].str.split(
        ' ', 1, expand=True)
    dataframe.drop('name', axis=1, inplace=True)
    return dataframe


def remove_prepended_zeroes(dataframe):
    '''
        Remove any zeros prepended to the price field
    '''
    # Convert price column to string if not already
    if not is_string_dtype(dataframe['price']):
        dataframe['price'].map(str)
    dataframe['price'] = [str(p).lstrip("0") for p in dataframe['price']]
    return dataframe


def add_above_100(dataframe):
    '''
        Create a new field named above_100, which is true if the price is
        strictly greater than 100

    '''
    dataframe['above_100'] = [True if float(
        p) > 100 else False for p in dataframe['price']]
    return dataframe


def save_csv(dataframe, filename):
    '''
        Save dataframe as csv
    '''
    dataframe.to_csv('processed_data/{}'.format(filename))


def datafram_pipeline(datafram):
    '''
        Data processing pipeline using pandas
    '''
    return datafram.pipe(drop_nan_names).pipe(split_names).pipe(
        remove_prepended_zeroes).pipe(add_above_100)
