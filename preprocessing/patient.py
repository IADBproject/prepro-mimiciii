"""
  Class: Table PATIENTS
"""
import math

import pandas as pd

from preprocessing.base import Base


class Patient(Base):
    """
        TABLE PATIENTS
    """

    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)

    def read_csv(self, criteria=None):
        """
            Read data from table CPTEVENTS
        """
    
        # Patient file name
        filename = self.config['FILE_DIR'] + self.config['IN_FNAME']['PATIENTS']
        usecols = ["ROW_ID", "SUBJECT_ID", "GENDER", "DOB", "DOD", "DOD_HOSP", "DOD_SSN", "EXPIRE_FLAG"]

        # Read from csv file
        #dateparse = lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S', errors='coerce')
        if not criteria:
            # dataframe = pd.read_csv(filename, encoding='latin1', usecols=usecols,\
            #     parse_dates=['DOB', 'DOD'], date_parser=dateparse)
            dataframe = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif criteria['nrows']:
            dataframe = pd.read_csv(filename, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])

        return dataframe

    def get_patients_by_ids(self, ids):
        """
            Get Patients by list of SUBJECT_ID
        """

        # Read Data from Patient
        df_pts = self.read_csv(criteria=None)

        # Select the SUBJECT_ID matche the SUBJECT_ID in HOSPITAL ADMISSIONS
        mask = df_pts['SUBJECT_ID'].isin(ids.tolist())
        df_pts = df_pts[mask]

        # Parse Date (computing time is faster than doing in read_csv)
        df_pts['DOB'] = pd.to_datetime(df_pts['DOB'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_pts['DOD'] = pd.to_datetime(df_pts['DOD'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Split ADMITTIME column to individual columns
        dob_sub_cols = {
            'DOB_YEAR': 'year',
            'DOB_MON': 'month',
            'DOB_DAY': 'day',
            'DOB_HOUR': 'hour',
            'DOB_MIN': 'min',
            'DOB_SEC': 'sec',
        }

        dod_sub_cols = {
            'DOD_YEAR': 'year',
            'DOD_MON': 'month',
            'DOD_DAY': 'day',
            'DOD_HOUR': 'hour',
            'DOD_MIN': 'min',
            'DOD_SEC': 'sec',
        }

        col_key = {
            'DOB': dob_sub_cols,
            'DOD': dod_sub_cols,
        }

        df_pts = self.split_date(df_pts, col_key)

        return df_pts

    def data_2_onehot(self, dataframe):
        """
            Convert dataframe to one-hot
        """

        # One-hot encoding column "GENDER"
        # Use pd.concat to join the new columns with original dataframe
        data_patients = pd.concat([dataframe, pd.get_dummies(dataframe['GENDER'], prefix='GENDER')], axis=1)
        # Drop the original
        data_patients.drop(['GENDER'], axis=1, inplace=True)

        # Write to CSV file
        #data_patients.to_csv(".\res\a_patient.csv")
        return data_patients
