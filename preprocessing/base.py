"""
    Base class
"""

import pandas as pd


class Base(object):
    """
        Base class
    """

    def __init__(self, **kwargs):
        self.config = kwargs

    def split_date(self, dataframe, col_key):
        """
            Split Date into Year, Month, Day Hour Minute and Second
        """

        # dataframe['ADM_YEAR'] = pd.to_datetime(dataframe[column], format='%Y-%m-%d %H:%M:%S').dt.year
        # dataframe['ADM_MON'] = dataframe[column].dt.month
        # dataframe['ADM_DAY'] = dataframe[column].dt.day
        # dataframe['ADM_HOUR'] = dataframe[column].dt.hour
        # dataframe['ADM_MIN'] = dataframe[column].dt.minute
        # dataframe['ADM_SEC'] = dataframe[column].dt.second

        for key, new_cols in col_key.items():
            for col_name, value in new_cols.items():
                if value == 'year':
                    dataframe[col_name] = dataframe[key].dt.year
                elif value == 'month':
                    dataframe[col_name] = dataframe[key].dt.month
                elif value == 'day':
                    dataframe[col_name] = dataframe[key].dt.day
                elif value == 'hour':
                    dataframe[col_name] = dataframe[key].dt.hour
                elif value == 'min':
                    dataframe[col_name] = dataframe[key].dt.minute
                elif value == 'sec':
                    dataframe[col_name] = dataframe[key].dt.second

        return dataframe

    def parse_date(self, str_date):
        """ Parse Date
        """

        return lambda x: pd.to_datetime(str(str_date), format='%Y-%m-%d %H:%M:%S', errors='coerce')
