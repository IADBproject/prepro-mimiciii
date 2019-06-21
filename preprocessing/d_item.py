"""
  Class: Table D_ITEMS
"""

import pandas as pd

from preprocessing.base import Base

LINKSTO_CHARTEVENT = 'CHARTEVENTS'
LINKTO_DATETIMEEVEENT = 'DATETIMEEVENTS'
LINKTO_INPUTEVENTS_CV = 'INPUTEVENTS_CV'
LINKTO_INPUTEVENTS_MV = 'INPUTEVENTS_MV'
LINKTO_MICROBIOLOGYEVENTS = 'MICROBIOLOGOYEVENTS'
LINKTO_OUTPUTEVENTS = 'outputevents'
LINKTO_PROCEDUREEVENTS_MV = 'PROCEDUREEVENTS_MV'


class DItem(Base):
    """
        TABLE D_ITEMS
    """

    def __init__(self, **kwargs):
        Base.__init__(self, **kwargs)

    def read_csv(self, criteria=None):
        """
            Read data from table D_ITEMS
        """

        # D_ITEMS file name
        filename = self.config['FILE_DIR'] + self.config['IN_FNAME']['D_ITEMS']
        usecols = ['ROW_ID', 'ITEMID', 'LABEL', 'ABBREVIATION', 'DBSOURCE',\
            'LINKSTO', 'CATEGORY', 'UNITNAME', 'PARAM_TYPE', 'CONCEPTID']

        # Set column dtype=str: Avoid ambiguity of Python interpreter
        col_dtype = {'ROW_ID':int, 'ITEMID':int, 'LABEL':str, 'ABBREVIATION':str, 'DBSOURCE':str,\
            'LINKSTO':str, 'CATEGORY':str, 'UNITNAME':str, 'PARAM_TYPE':str, 'CONCEPTID':str}

        # Read from csv file
        df_ditem = pd.read_csv(filename, dtype=col_dtype, encoding='latin1', usecols=usecols)
        df_ditem = df_ditem.add_prefix(self.config['PREFIX_DITEM'])

        # if criteria is not None:
        #     # Conditions
        #     subject_id = data['SUBJECT_ID'] == criteria['SUBJECT_ID']
        #     hamd_id = data['HADM_ID'] == criteria['HADM_ID']

        #     # SELECT AN ICU STAY
        #     data = data[subject_id & hamd_id]
        return df_ditem

    def get_ditems_outevents_by_itemid(self, criteria=None):
        """
            Get Patients by list of SUBJECT_ID
        """

        # Read D_ITEMS from csv file
        df_ditems = self.read_csv(criteria=None)

        mask = (df_ditems[self.config['PREFIX_DITEM'] + 'ITEMID'].isin(\
            criteria[self.config['PREFIX_DITEM'] + 'ITEMID'].tolist()))\
            & (df_ditems[self.config['PREFIX_DITEM'] + 'LINKSTO'] == self.config['PREFIX_DITEM'] + LINKTO_OUTPUTEVENTS)
        df_ditems_outevents = df_ditems[mask]

        return df_ditems_outevents

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

        # One-hot COSTCENTER
        #COSTCENTER = pd.get_dummies(data_cptevents['COSTCENTER'], prefix='COSTCENTER')
        return one_hot_data
