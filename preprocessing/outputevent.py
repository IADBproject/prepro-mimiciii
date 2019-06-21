# -*- coding: utf-8 -*-

import pandas as pd

from preprocessing.base import Base


class OutputEvent(Base):
    """
        Table: OUTPUTEVENTS
    """

    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)

    def read_csv(self, criteria=None):
        """
            Read data from table OUTPUTEVENTS
        """

        filename = self.config['FILE_DIR'] + self.config['IN_FNAME']['OUTPUTEVENTS']
        usecols = ['ROW_ID', 'SUBJECT_ID',	'HADM_ID', 'ICUSTAY_ID', 'CHARTTIME', 'ITEMID',\
            'VALUE', 'VALUEUOM', 'STORETIME', 'CGID', 'STOPPED', 'NEWBOTTLE', 'ISERROR']

        # Read from csv file
        if not criteria:
            df_outputevs = pd.read_csv(filename, encoding='latin1', usecols=usecols)
        elif criteria['nrows'] is not None:
            df_outputevs = pd.read_csv(filename, encoding='latin1', usecols=usecols, nrows=criteria['nrows'])

        return df_outputevs

    def get_outputevents_by_pthadmicu(self, criteria=None):
        """ Retrieve OUTPUTEVENTS matching the give hospital admission
        """

        # Read Data from OutputEvents
        df_outputevs = self.read_csv(criteria=None)
        # Add prefix to column's name
        df_outputevs = df_outputevs.add_prefix(self.config['PREFIX_OUEV'])

        # Filter Dataframe by SUBJECT_ID, HADM_ID and ICUSTAY_ID
        # mask = ''
        # if criteria:
        #     for k_col, value in criteria.items():
        #         if mask:
        #             mask += ' & ' + df_outputevs[k_col].isin(value.tolist())
        #         else:
        #             mask = df_outputevs[k_col].isin(value.tolist())

        mask = (df_outputevs[self.config['PREFIX_OUEV'] + 'SUBJECT_ID'].isin(\
            criteria[self.config['PREFIX_OUEV'] + 'SUBJECT_ID'].tolist()))\
            & (df_outputevs[self.config['PREFIX_OUEV'] + 'HADM_ID'].isin(\
                criteria[self.config['PREFIX_OUEV'] + 'HADM_ID'].tolist()))

        # mask = (df_outputevs[self.config['PREFIX_OUEV'] + 'SUBJECT_ID'].isin(\
        #     criteria[self.config['PREFIX_OUEV'] + 'SUBJECT_ID'].tolist()))\
        #     & (df_outputevs[self.config['PREFIX_OUEV'] + 'HADM_ID'].isin(\
        #         criteria[self.config['PREFIX_OUEV'] + 'HADM_ID'].tolist()))\
        #         & (df_outputevs[self.config['PREFIX_OUEV'] + 'ICUSTAY_ID'].isin(\
        #             criteria[self.config['PREFIX_OUEV'] + 'ICUSTAY_ID'].tolist()))
        if mask is not None:
            df_outputevs = df_outputevs[mask]

        return df_outputevs
