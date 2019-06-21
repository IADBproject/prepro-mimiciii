"""
  Class: Table ADMISSIONS
"""


# import blaze
import pandas as pd

from preprocessing.base import Base

class Admission(Base):
    """
        TABLE ADMISSIONS
    """

    def read_csv(self, criteria=None):
        """
            Read data from table ADMISSIONS
        """

        # Admission file name
        filename = self.config['FILE_DIR'] + self.config['IN_FNAME']['ADMISSIONS']

        # Define columns to read from file
        usecols = ["ROW_ID", "SUBJECT_ID", "HADM_ID", "ADMITTIME", "DISCHTIME",\
            "DEATHTIME", "ADMISSION_TYPE", "ADMISSION_LOCATION", "DISCHARGE_LOCATION",\
                "INSURANCE", "LANGUAGE",	"RELIGION", "MARITAL_STATUS", "ETHNICITY",\
                    "EDREGTIME", "EDOUTTIME", "HOSPITAL_EXPIRE_FLAG", "HAS_CHARTEVENTS_DATA"]

        # Read from csv file
        # df_adms = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        # df_adms = df_adms.add_prefix(self.config['PREFIX_HADM'])

        # Read from csv file
        if not criteria:
            # df_adms = pd.read_csv(filename, parse_dates=['ADMITTIME', 'DISCHTIME', 'DEATHTIME'],\
            # date_parser=dateparse, encoding='latin1', usecols=usecols)
            df_adms = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif self.config['CONST']['N_ROWS'] in criteria:
            # df_adms = pd.read_csv(filename, parse_dates=['ADMITTIME', 'DISCHTIME', 'DEATHTIME'],\
            # date_parser=dateparse, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])
            df_adms = pd.read_csv(filename, encoding='latin1', usecols=usecols,\
                nrows=criteria[self.config['CONST']['N_ROWS']])
        else: 
            df_adms = pd.read_csv(filename, encoding='latin1', usecols=usecols)

        # if criteria is not None:
        #     # Conditions
        #     subject_id = df_adms['SUBJECT_ID'] == criteria['SUBJECT_ID']
        #     # hamd_id = data_admissions['HADM_ID'] == criteria['HADM_ID']
        #     # SELECT AN ICU STAY
        #     df_adms = df_adms[subject_id]

        return df_adms

    def data_2_onehot(self, dataframe):
        """
            Convert dataframe to one-hot
        """

        # One-hot admission location
        data_adms = pd.concat([dataframe, pd.get_dummies(dataframe['ADMISSION_TYPE'], prefix='ADMT')], axis=1)
        # One-hot insurance
        data_adms = pd.concat([data_adms, pd.get_dummies(dataframe['INSURANCE'], prefix='INS')], axis=1)
        # One-hot MARITAL STATUS
        data_adms = pd.concat([data_adms, pd.get_dummies(dataframe['MARITAL_STATUS'], prefix='MARSTA')], axis=1)

        return data_adms

    def get_admissions(self, criteria=None):
        """ Read Admission record from CSV
        """

        # Get Admssions of the year having highest number of frequency
        if self.config['PARAM']['READ_ALL_RECORDS'] == self.config['CONST']['K_NO']:
            return self.get_admissions_by_year(criteria)
        else:
            return self.get_all_admissions(criteria)

    def get_admissions_by_year(self, criteria=None):
        """
            Read data from table ADMISSIONS
        """

        # # Get only year
        # # dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        # # dateparse = lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # # Add Prefix to columns name
        df_adms = self.read_csv(criteria)
        df_adms = df_adms.add_prefix(self.config['PREFIX_HADM'])

        # Parse Date (computing time is faster than doing in read_csv)
        df_adms[self.config['PREFIX_HADM'] + 'ADMITTIME'] = pd.to_datetime(
            df_adms[self.config['PREFIX_HADM'] + 'ADMITTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_adms[self.config['PREFIX_HADM'] + 'DISCHTIME'] = pd.to_datetime(
            df_adms[self.config['PREFIX_HADM'] + 'DISCHTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_adms[self.config['PREFIX_HADM'] + 'DEATHTIME'] = pd.to_datetime(
            df_adms[self.config['PREFIX_HADM'] + 'DEATHTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Split ADMITTIME column to individual columns
        admitime_sub_cols = {
            self.config['PREFIX_HADM'] + 'ADM_YEAR': 'year',
            self.config['PREFIX_HADM'] + 'ADM_MON': 'month',
            self.config['PREFIX_HADM'] + 'ADM_DAY': 'day',
            self.config['PREFIX_HADM'] + 'ADM_HOUR': 'hour',
            self.config['PREFIX_HADM'] + 'ADM_MIN': 'min',
            self.config['PREFIX_HADM'] + 'ADM_SEC': 'sec',
        }

        dischtime_sub_cols = {
            self.config['PREFIX_HADM'] + 'DISCH_YEAR': 'year',
            self.config['PREFIX_HADM'] + 'DISCH_MON': 'month',
            self.config['PREFIX_HADM'] + 'DISCH_DAY': 'day',
            self.config['PREFIX_HADM'] + 'DISCH_HOUR': 'hour',
            self.config['PREFIX_HADM'] + 'DISCH_MIN': 'min',
            self.config['PREFIX_HADM'] + 'DISCH_SEC': 'sec',
        }

        deathtime_sub_cols = {
            self.config['PREFIX_HADM'] + 'DEATHYEAR': 'year',
            self.config['PREFIX_HADM'] + 'DEATHMON': 'month',
            self.config['PREFIX_HADM'] + 'DEATHDAY': 'day',
            self.config['PREFIX_HADM'] + 'DEATHHOUR': 'hour',
            self.config['PREFIX_HADM'] + 'DEATHMIN': 'min',
            self.config['PREFIX_HADM'] + 'DEATHSEC': 'sec',
        }

        col_key = {
            self.config['PREFIX_HADM'] + 'ADMITTIME': admitime_sub_cols,
            self.config['PREFIX_HADM'] + 'DISCHTIME': dischtime_sub_cols,
            self.config['PREFIX_HADM'] + 'DEATHTIME': deathtime_sub_cols
        }

        df_adms = self.split_date(df_adms, col_key)

        # index_max = df_adms_gb.loc[df_adms_gb['ROW_ID'].idxmax()]
        df_adms_gb = df_adms.groupby([self.config['PREFIX_HADM'] + 'ADM_YEAR']).count()

        # Get Year with Highest Frequency of admission
        max_freq_year = df_adms_gb[self.config['PREFIX_HADM'] + 'ROW_ID'].idxmax()
        lower_date = pd.to_datetime(str(max_freq_year) + '-01-01 00:00:00',\
            format='%Y-%m-%d %H:%M:%S', infer_datetime_format=True, errors='coerce')
        upper_date = pd.to_datetime(str(max_freq_year)+ '-12-31 11:59:59',\
            format='%Y-%m-%d %H:%M:%S', infer_datetime_format=True, errors='coerce')

        # max = df_adms_gb['ROW_ID'].max()
        # idxmax = df_adms_gb.loc[df_adms_gb['ROW_ID'].idxmax()]
        # min_date = lower_date <= df_adms['ADMITTIME']
        # max_date = df_adms['ADMITTIME'] <= upper_date
        mask = (df_adms[self.config['PREFIX_HADM'] + 'ADMITTIME'] >= lower_date)\
            & (df_adms[self.config['PREFIX_HADM'] + 'ADMITTIME'] <= upper_date)
        result = df_adms[mask]

        return result
    
    def get_all_admissions(self, criteria=None):
        """
            Read data from table ADMISSIONS
        """

        # # Get only year
        # # dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        # # dateparse = lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # # Add Prefix to columns name
        df_adms = self.read_csv(criteria)
        df_adms = df_adms.add_prefix(self.config['PREFIX_HADM'])

        # Parse Date (computing time is faster than doing in read_csv)
        df_adms[self.config['PREFIX_HADM'] + 'ADMITTIME'] = pd.to_datetime(
            df_adms[self.config['PREFIX_HADM'] + 'ADMITTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_adms[self.config['PREFIX_HADM'] + 'DISCHTIME'] = pd.to_datetime(
            df_adms[self.config['PREFIX_HADM'] + 'DISCHTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_adms[self.config['PREFIX_HADM'] + 'DEATHTIME'] = pd.to_datetime(
            df_adms[self.config['PREFIX_HADM'] + 'DEATHTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Split ADMITTIME column to individual columns
        admitime_sub_cols = {
            self.config['PREFIX_HADM'] + 'ADM_YEAR': 'year',
            self.config['PREFIX_HADM'] + 'ADM_MON': 'month',
            self.config['PREFIX_HADM'] + 'ADM_DAY': 'day',
            self.config['PREFIX_HADM'] + 'ADM_HOUR': 'hour',
            self.config['PREFIX_HADM'] + 'ADM_MIN': 'min',
            self.config['PREFIX_HADM'] + 'ADM_SEC': 'sec',
        }

        dischtime_sub_cols = {
            self.config['PREFIX_HADM'] + 'DISCH_YEAR': 'year',
            self.config['PREFIX_HADM'] + 'DISCH_MON': 'month',
            self.config['PREFIX_HADM'] + 'DISCH_DAY': 'day',
            self.config['PREFIX_HADM'] + 'DISCH_HOUR': 'hour',
            self.config['PREFIX_HADM'] + 'DISCH_MIN': 'min',
            self.config['PREFIX_HADM'] + 'DISCH_SEC': 'sec',
        }

        deathtime_sub_cols = {
            self.config['PREFIX_HADM'] + 'DEATHYEAR': 'year',
            self.config['PREFIX_HADM'] + 'DEATHMON': 'month',
            self.config['PREFIX_HADM'] + 'DEATHDAY': 'day',
            self.config['PREFIX_HADM'] + 'DEATHHOUR': 'hour',
            self.config['PREFIX_HADM'] + 'DEATHMIN': 'min',
            self.config['PREFIX_HADM'] + 'DEATHSEC': 'sec',
        }

        col_key = {
            self.config['PREFIX_HADM'] + 'ADMITTIME': admitime_sub_cols,
            self.config['PREFIX_HADM'] + 'DISCHTIME': dischtime_sub_cols,
            self.config['PREFIX_HADM'] + 'DEATHTIME': deathtime_sub_cols
        }

        # Split Data into Year, Month, Day
        df_adms = self.split_date(df_adms, col_key)

        return df_adms
