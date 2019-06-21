"""
  Class: PREPROCESSING STEP 2
"""


# import blaze
import pandas as pd

from preprocessing.step2.base import Base

class DataReader(Base):
    """
        TABLE ADMISSIONS
    """

    def get_tbl_header(self, filename):
        """
            Get table header from cvs file
        """

        return pd.read_csv(filename, index_col=0, nrows=0).columns.tolist()

    def get_pt_hamd_icus(self, criteria=None):
        """
            Get Patients based on list of SUBJECT ID
        """

        filename = self.config['FILE_DIR_S2'] + self.config['IN_FNAME']['CSV_OUT_PTS_ADMS_ICUS']
        usecols = self.get_tbl_header(filename)

        # Read from csv file
        if not criteria:
            df_pt_hamds = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif criteria['nrows'] is not None:
            df_pt_hamds = pd.read_csv(filename, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])

        return df_pt_hamds

    def get_outputevents(self, criteria=None):
        """
            Get CSV_OUTPUTEVS by SUBJECT_ID AND HADM_ID
        """

        filename = self.config['FILE_DIR_S2'] + self.config['IN_FNAME']['CSV_OUT_OUTPUTEVENTS']
        usecols = self.get_tbl_header(filename)

        criteria = {}

        # Read from csv file
        if not criteria:
            df_outputevs = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif criteria['nrows'] is not None:
            df_outputevs = pd.read_csv(filename, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])

        # Parse Date (computing time is faster than doing in read_csv)
        df_outputevs[self.config['PREFIX_OUEV'] + 'CHARTTIME'] = pd.to_datetime(
            df_outputevs[self.config['PREFIX_OUEV'] + 'CHARTTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_outputevs[self.config['PREFIX_OUEV'] + 'STORETIME'] = pd.to_datetime(
            df_outputevs[self.config['PREFIX_OUEV'] + 'STORETIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        return df_outputevs

    def get_chartevents(self, criteria=None):
        """
            Get CHARTEVENTS
        """

        filename = self.config['FILE_DIR_S2'] + self.config['IN_FNAME']['CSV_OUT_CHARTEVENTS']
        usecols = self.get_tbl_header(filename)

        criteria = {}

        # Read from csv file
        if not criteria:
            df_charevs = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif criteria['nrows'] is not None:
            df_charevs = pd.read_csv(filename, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])

        # Parse Date (computing time is faster than doing in read_csv)
        df_charevs[self.config['PREFIX_CHEV'] + 'CHARTTIME'] = pd.to_datetime(
            df_charevs[self.config['PREFIX_CHEV'] + 'CHARTTIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')
        df_charevs[self.config['PREFIX_CHEV'] + 'STORETIME'] = pd.to_datetime(
            df_charevs[self.config['PREFIX_CHEV'] + 'STORETIME'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        return df_charevs
