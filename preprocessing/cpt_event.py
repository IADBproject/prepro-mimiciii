"""
  Class: Table CPTEVENTS
"""

import pandas as pd

from preprocessing.base import Base


class CPTEvent(Base):
    """
        TABLE CPTEVENTS
    """

    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)

    def read_csv(self, criteria=None):
        """
            Read data from table CPTEVENTS
        """

        file_cptevents = self.config["file_directory"] + "CPTEVENTS.csv"
        usecols = ["SUBJECT_ID", "HADM_ID", "COSTCENTER", "CHARTDATE",\
            "CPT_CD", "CPT_NUMBER", "CPT_SUFFIX", "TICKET_ID_SEQ",\
                'SECTIONHEADER', 'SUBSECTIONHEADER', 'DESCRIPTION']


        # Set column dtype=str: Avoid ambiguity of Python interpreter
        col_dtype = {
                        "CHARTDATE": str, "CPT_CD": str, "CPT_NUMBER": str,
                        "COSTCENTER": str, "CPT_SUFFIX": str, "TICKET_ID_SEQ": str,
                        'SECTIONHEADER': str, 'SUBSECTIONHEADER':str, 'DESCRIPTION':str}

        # Read from csv file
        data = pd.read_csv(file_cptevents, dtype=col_dtype,\
            encoding='latin1', usecols=usecols)

        # if criteria is not None:
        #     # Conditions
        #     subject_id = data['SUBJECT_ID'] == criteria['SUBJECT_ID']
        #     hamd_id = data['HADM_ID'] == criteria['HADM_ID']

        #     # SELECT AN ICU STAY
        #     data = data[subject_id & hamd_id]

        return data

    def get_cptevents_by_subject_hamd(self, criteria=None):
        """
            Get Patients by list of SUBJECT_ID
        """

        df_cptevents = self.read_csv(criteria=None)

        mask = (df_cptevents['SUBJECT_ID'].isin(criteria['SUBJECT_ID'].tolist()))\
            & (df_cptevents['HADM_ID'].isin(criteria['HADM_ID'].tolist()))
        df_cptevents = df_cptevents[mask]

         # Parse Date (computing time is faster than doing in read_csv)
        df_cptevents['CHARTDATE'] = pd.to_datetime(df_cptevents['CHARTDATE'],\
            format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Split ADMITTIME column to individual columns
        chart_sub_cols = {
            'CHART_YEAR': 'year',
            'CHART_MON': 'month',
            'CHART_DAY': 'day',
            'CHART_HOUR': 'hour',
            'CHART_MIN': 'min',
            'CHART_SEC': 'sec',
        }

        col_key = {
            'CHARTDATE': chart_sub_cols
        }

        df_cptevents = self.split_date(df_cptevents, col_key)

        return df_cptevents

    def data_2_onehot(self, dataframe):
        """
            Convert dataframe to one-hot
        """

        # One-hot CPT_CD
        one_hot_data = pd.concat([dataframe, pd.get_dummies(dataframe['CPT_CD'],\
             prefix='CPT_CD')], axis=1)

        # dropcols_cptevents = [one_hot_data_cptevents.columns[0], "ROW_ID",\
        #  "COSTCENTER", "CHARTDATE", "CPT_CD", "CPT_NUMBER", "CPT_SUFFIX", "TICKET_ID_SEQ"]
        dropcols = ["COSTCENTER", "CHARTDATE", "CPT_CD", "CPT_NUMBER",\
             "CPT_SUFFIX", "TICKET_ID_SEQ"]

        #one_hot_data_cptevents.drop(one_hot_data_cptevents.columns[0], axis=1)
        one_hot_data = one_hot_data.drop(dropcols, axis=1)

        # GroupBy SUBJECT_ID & HADM_ID
        one_hot_data = one_hot_data.groupby(['SUBJECT_ID', 'HADM_ID'], as_index=True)

        #data_cptevents = data_cptevents.groupby(data_cptevents['SUBJECT_ID'])
        #data_cptevents.sum().to_csv("data_cptevents.csv")
        #one_hot_data_cptevents.drop(one_hot_data_cptevents, axis=1)
        # one_hot_data_cptevents.any().to_csv("data_cptevents.csv")

        # One-hot COSTCENTER
        #COSTCENTER = pd.get_dummies(data_cptevents['COSTCENTER'], prefix='COSTCENTER')
        return one_hot_data
