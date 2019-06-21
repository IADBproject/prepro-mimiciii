"""
  Class: Table TABLE ICUSTAY
"""

import pandas as pd

from preprocessing.base import Base


class ICUStay(Base):
    """
        TABLE ICUSTAY
    """

    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)

    def read_csv(self, criteria=None):
        """
            Read data from table CPTEVENTS
        """

        # ICUSTAYS file name
        filename = self.config['FILE_DIR'] + self.config['IN_FNAME']['ICUSTAYS']
        # Set column dtype=str: Avoid ambiguity of Python interpreter
        # col_dtype = {
        #                 "CHARTDATE": str, "CPT_CD": str, "CPT_NUMBER": str,
        #                 "COSTCENTER": str, "CPT_SUFFIX": str, "TICKET_ID_SEQ": str}
        usecols = ["ROW_ID", "SUBJECT_ID", "HADM_ID", "ICUSTAY_ID", "DBSOURCE",\
                    "FIRST_CAREUNIT", "LAST_CAREUNIT", "FIRST_WARDID", "LAST_WARDID",\
                        "INTIME", "OUTTIME",	"LOS"]
    
        # Read from csv file
        df_icustay = pd.read_csv(filename, encoding='latin1', usecols=usecols)

        return df_icustay

    def get_icustays_by_subject_hamd(self, criteria=None):
        """
            Get ICUStay by list of SUBJECT_ID and HADM_ID
        """

        # Read Data from Icustay
        df_icustays = self.read_csv(criteria=None)
        df_icustays = df_icustays.add_prefix(self.config['PREFIX_ICU'])

        mask = (df_icustays[self.config['PREFIX_ICU'] + 'SUBJECT_ID'].isin(\
            criteria[self.config['PREFIX_ICU'] + 'SUBJECT_ID'].tolist()))\
            & (df_icustays[self.config['PREFIX_ICU'] + 'HADM_ID'].isin(\
                criteria[self.config['PREFIX_ICU'] + 'HADM_ID'].tolist()))
        df_icustays = df_icustays[mask]

        # Parse Date (computing time is faster than doing in read_csv)
        df_icustays[self.config['PREFIX_ICU'] + 'INTIME'] = pd.to_datetime(\
            df_icustays[self.config['PREFIX_ICU'] + 'INTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_icustays[self.config['PREFIX_ICU'] + 'OUTTIME'] = pd.to_datetime(\
            df_icustays[self.config['PREFIX_ICU'] + 'OUTTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Split ADMITTIME column to individual columns
        intime_sub_cols = {
            self.config['PREFIX_ICU'] + 'INTTIME_YEAR': 'year',
            self.config['PREFIX_ICU'] + 'INTTIME_MON': 'month',
            self.config['PREFIX_ICU'] + 'INTTIME_DAY': 'day',
            self.config['PREFIX_ICU'] + 'INTTIME_HOUR': 'hour',
            self.config['PREFIX_ICU'] + 'INTTIME_MIN': 'min',
            self.config['PREFIX_ICU'] + 'INTTIME_SEC': 'sec',
        }

        outtime_sub_cols = {
            self.config['PREFIX_ICU'] + 'OUTTIME_YEAR': 'year',
            self.config['PREFIX_ICU'] + 'OUTTIME_MON': 'month',
            self.config['PREFIX_ICU'] + 'OUTTIME_DAY': 'day',
            self.config['PREFIX_ICU'] + 'OUTTIME_HOUR': 'hour',
            self.config['PREFIX_ICU'] + 'OUTTIME_MIN': 'min',
            self.config['PREFIX_ICU'] + 'OUTTIME_SEC': 'sec',
        }

        col_key = {
            self.config['PREFIX_ICU'] + 'INTIME': intime_sub_cols,
            self.config['PREFIX_ICU'] + 'OUTTIME': outtime_sub_cols,
        }

        df_icustays = self.split_date(df_icustays, col_key)

        return df_icustays

    def data_2_onehot(self, dataframe):
        """
            Convert dataframe to one-hot
        """

        # One-hot CPT_CD
        one_hot_data_cptevents = pd.concat([dataframe, pd.get_dummies(dataframe['CPT_CD'],\
             prefix='CPT_CD')], axis=1)

        # dropcols_cptevents = [one_hot_data_cptevents.columns[0], "ROW_ID",\
        #  "COSTCENTER", "CHARTDATE", "CPT_CD", "CPT_NUMBER", "CPT_SUFFIX", "TICKET_ID_SEQ"]
        dropcols_cptevents = ["COSTCENTER", "CHARTDATE", "CPT_CD", "CPT_NUMBER",\
             "CPT_SUFFIX", "TICKET_ID_SEQ"]

        #one_hot_data_cptevents.drop(one_hot_data_cptevents.columns[0], axis=1)
        one_hot_data_cptevents = one_hot_data_cptevents.drop(dropcols_cptevents, axis=1)

        # GroupBy SUBJECT_ID & HADM_ID
        one_hot_data_cptevents = one_hot_data_cptevents.groupby(['SUBJECT_ID', 'HADM_ID'], as_index=False)

        # one_hot_data_cptevents.any().to_csv("data_cptevents.csv")
