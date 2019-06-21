"""
    Preprocessing
"""

import time
import datetime
import math

import pandas as pd
import numpy as np

from preprocessing.step2.data_reader import DataReader

from utils.file_helper import FileHelper

class DataCompilation:
    """
        This step combines OUTPUTEVENTS, CHARTEVENTS, AND PTS_ADMS_ICUS, THEN
        Generate number of hospital procedures applied to each patient during their ICU Stay.
        The events (hospital procedures) are group by a window size of 24 hours
    """

    def __init__(self, **kwargs):
        self.config = kwargs
        self.datareader = DataReader(**self.config)

    def start_process(self):
        """
            Read data from tables: PATIENTS, ADMISSIONS, CPTEVENT
        """

        # Read admissions groupby date and
        df_pt_adm_icus = self.get_pt_hamd_icus()
        df_output_evs = self.get_outputevents()

        # Save the dico to dataframe
        pros_by_window = self.__grouppros_by_interval(df_pt_adm_icus, df_output_evs, ev_unit=self.config['CONST']['HUNIT_ICU'])
        dico_pros_dt, index = self.__pros_todico(pros_by_window)
        df_temp = pd.DataFrame(dico_pros_dt, index=index)
        # Save Admissions to file
        # Filename of compiling tables: PATIENTS, ADMISSIONS, ICUSTAYS
        filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['OE_BY_DATE_INTERVAL']
        FileHelper.save_to_csv(df_temp, filename)

        # Merge Tables: PATIENTS, ADMISSIONS and ICUSTAYS
        # Conditions:
        #   SUBJECT_ID : Patient's ID
        #   HADM_SUBJECT_ID: Patient's ID of table ADMISSIONS
        col_subject_id = self.config['PREFIX_HADM'] + 'SUBJECT_ID'
        col_hadm_id = self.config['PREFIX_HADM'] + 'HADM_ID'
        col_icustay_id = self.config['PREFIX_ICU'] + 'ICUSTAY_ID'

        # Retrieve Outputevents
        # Conditions:
        # ----------
        #   col_subject_id, col_hadm_id, col_icustay_id
        left = [col_subject_id, col_hadm_id, col_icustay_id]
        right = ['subject_id', 'hadm_id', 'icustay_id']
        filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['PT_ADM_ICU_OUTEVENT']
        df_pt_adm_icu_outevent = self.merge_df(df_pt_adm_icus, df_temp, left=left, right=right,\
            how='right', out_filename=filename)

        df_chart_evs = self.get_chartevents()
        # Save the dico to dataframe
        pros_by_window_2 = self.__grouppros_by_interval(df_pt_adm_icus, df_chart_evs,\
            ev_unit=self.config['CONST']['HUNIT_CHAREV'], outevent=False)
        dico_pros_dt2, index2 = self.__pros_todico(pros_by_window_2)
        df_temp2 = pd.DataFrame(dico_pros_dt2, index=index2)
        # Save Admissions to file
        filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['CHEV_BY_DATE_INTERVAL']
        FileHelper.save_to_csv(df_temp2, filename)

        # Retrieve Chartevents
        # Conditions:
        # ----------
        #   col_subject_id, col_hadm_id, col_icustay_id
        left = [col_subject_id, col_hadm_id, col_icustay_id]
        right = ['subject_id', 'hadm_id', 'icustay_id']
        filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['PT_ADM_ICUS_CHAREVS']
        df_pt_adm_icu_charevs = self.merge_df(df_pt_adm_icus, df_temp2, left=left, right=right,\
            how='right', out_filename=filename)

        # Append dataframe amd-icustay-outputevents to amd-icustay-chartevents
        # Conditions:
        # ----------
        #   OutputEvent (filtered by subject_id, hamd_id, icu_stay)
        #   & ChartEvents (filtered by subject_id, hamd_id, icu_stay)
        df_pt_adm_icu_outevs_charevs = df_pt_adm_icu_outevent.append(df_pt_adm_icu_charevs, sort=True)
        # Save df_pt_adm_icu_outevs_charevs to file
        filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['OUTEVENT_CHAREVS']
        FileHelper.save_to_csv(df_pt_adm_icu_outevs_charevs, filename)

        # Retrieve all rows in Admissions where (subject_id, hamd_id, icu_id) do not match
        # (subject_id, hamd_id, icu_id) of U(outputevents, chartevents)
        # left = [col_subject_id, col_hadm_id, col_icustay_id]
        # right = ['subject_id', 'hadm_id', 'icustay_id']
        temp_df = df_pt_adm_icus[~df_pt_adm_icus[col_subject_id].isin(df_pt_adm_icu_outevs_charevs['subject_id'])\
            & ~df_pt_adm_icus[col_hadm_id].isin(df_pt_adm_icu_outevs_charevs['hadm_id'])\
            & ~df_pt_adm_icus[col_icustay_id].isin(df_pt_adm_icu_outevs_charevs['icustay_id'])]

        # Add columns to temp_df
        # Conditions
        # ---------
        # subject_id	hadm_id	icustay_id	unit	procedure
        # temp_df.loc[:, 'subject_id_1'] = temp_df[col_subject_id]
        # temp_df.loc[:, 'hadm_id_1'] = temp_df[col_hadm_id]
        # temp_df.loc[:, 'icustay_id_1'] = temp_df[col_icustay_id]
        # temp_df.loc[:, 'unit_1'] = ''
        # temp_df.loc[:, 'procedure_1'] = ''

        # Save Admissions to file
        filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['TEMP_DF']
        FileHelper.save_to_csv(temp_df, filename)

        ##########
        # Final Merge
        ##########
        # 
        df_pt_adm_icu_outevs_charevs = df_pt_adm_icu_outevs_charevs.append(temp_df, sort=True)
        #df_pt_adm_icu_outevs_charevs = temp_df.append(df_pt_adm_icu_outevs_charevs, sort=True)

        ### Count number of records
        self.config['PARAM']['NUM_ROWS'] = len(df_pt_adm_icu_outevs_charevs.index)

        # Save NUM_EVENTS_WINSIZE_24H to file
        # filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['OUT_NUM_EVENTS_WINSIZE_24H']
        filename = self.config['OUT_DIR_S2'] + str(self.config['PARAM']['LIMIT_NUM_PATIENT']) + '_' +\
            self.config['OUT_FNAME']['OUT_NUM_EVENTS_WINSIZE_24H']
        FileHelper.save_to_csv(df_pt_adm_icu_outevs_charevs, filename)

        ### Sharp number of patients for output based on the criteria
        self.__output_num_patient_by_limit(df_pt_adm_icu_outevs_charevs)

    def __grouppros_by_interval(self, df_pt_adm_icus, df_procedures, ev_unit=None, outevent=True):
        """
        """

        if ev_unit is None:
            ev_unit=self.config['CONST']['HUNIT_ICU']

        col_pai_subject_id = self.config['PREFIX_HADM'] + 'SUBJECT_ID'
        col_pai_hadm_id = self.config['PREFIX_HADM'] + 'HADM_ID'
        col_pai_icustay_id = self.config['PREFIX_ICU'] + 'ICUSTAY_ID'

        if outevent:
            col_subject_id = self.config['PREFIX_OUEV'] + 'SUBJECT_ID'
            col_hadm_id = self.config['PREFIX_OUEV'] + 'HADM_ID'
            col_icustay_id = self.config['PREFIX_OUEV'] + 'ICUSTAY_ID'
            col_charttime = self.config['PREFIX_OUEV'] + 'CHARTTIME'
            col_itemid = self.config['PREFIX_OUEV'] + 'ITEMID'
        else:
            col_subject_id = self.config['PREFIX_CHEV'] + 'SUBJECT_ID'
            col_hadm_id = self.config['PREFIX_CHEV'] + 'HADM_ID'
            col_icustay_id = self.config['PREFIX_CHEV'] + 'ICUSTAY_ID'
            col_charttime = self.config['PREFIX_CHEV'] + 'CHARTTIME'
            col_itemid = self.config['PREFIX_CHEV'] + 'ITEMID'

        # List of pair wise outevents & date
        pros_by_window = []
        for _, row in df_pt_adm_icus.iterrows():

            subject_id = row[col_pai_subject_id]
            hadm_id = row[col_pai_hadm_id]
            icustay_id = row[col_pai_icustay_id]

            # Get outevents matching SUBJECT_ID and HAMD_ID
            mask = (df_procedures[col_subject_id] == subject_id) & (df_procedures[col_hadm_id] == hadm_id)\
                    & (df_procedures[col_icustay_id] == icustay_id)
            df_output_ev = df_procedures[mask]

            # Check if no dataframe
            if df_output_ev.empty:
                continue

            # Sort CHARTIME by non decreasing order
            df_output_ev = df_output_ev.sort_values(col_charttime, ascending=True)

            # Group the output events by WINDOW of length (1 day or 7 days)
            WIN_SIZE = 1*24*60*60
            # Get first charttime in the list
            start_charttime = df_output_ev.iloc[0][col_charttime]
            # Get the last charttime in the list
            end_chartime = df_output_ev.iloc[-1][col_charttime]

            # Calculate the length of stay by number of days
            len_of_stay = end_chartime - start_charttime
            len_of_stay = math.ceil((end_chartime - start_charttime)/pd.offsets.Second(1))

            # Unit
            unit = ''
            if icustay_id:
                unit = ev_unit

            # 
            if len_of_stay <= WIN_SIZE:
                # mask = (df_output_ev[self.config['PREFIX_OUEV'] + 'CHARTTIME'].isin(date_interval))
                mask = (df_output_ev[col_charttime] >= start_charttime)\
                    &(df_output_ev[col_charttime] <= end_chartime)
                df_evs_by_win = df_output_ev[mask].sort_values(col_charttime, ascending=True)
                df_evs_by_win = zip(df_evs_by_win[col_itemid],\
                    df_evs_by_win[col_charttime].dt.strftime('%Y-%m-%d %H:%M:%S'))

                # Add to list tuple(outevent, charttime)
                makeitastring = ','.join(map(str, set(df_evs_by_win)))
                pros_by_window.append((subject_id, hadm_id, icustay_id, unit, makeitastring))
            else:
                # Number of date interval given the window size
                nbr_temp = math.ceil(len_of_stay/WIN_SIZE)

                for i in range(0, nbr_temp, 1):
                    if i == 0:
                        start_charttime += datetime.timedelta(seconds=0)
                        end_chartime = start_charttime + datetime.timedelta(seconds=WIN_SIZE-1)
                    else:
                        start_charttime += datetime.timedelta(seconds=WIN_SIZE)
                        end_chartime += datetime.timedelta(seconds=WIN_SIZE)

                    # Get the event by interval
                    mask = (df_output_ev[col_charttime] >= start_charttime)\
                        & (df_output_ev[col_charttime] <= end_chartime)
                    df_evs_by_win = df_output_ev[mask]
                    # df_evs_by_win = zip(df_evs_by_win[oe_itemid], str(df_evs_by_win[oe_charttime]))
                    df_evs_by_win = zip(df_evs_by_win[col_itemid],\
                        df_evs_by_win[col_charttime].dt.strftime('%Y-%m-%d %H:%M:%S'))

                    makeitastring = ','.join(map(str, set(df_evs_by_win)))
                    pros_by_window.append((subject_id, hadm_id, icustay_id, unit, makeitastring))

        return pros_by_window

    def __pros_todico(self, pros_by_window):
        """
            From list of procedures to dictionary

            Returns
            -------
            output: data for Dataframe 
        """

        # Dictionary to store result by group of date interval
        dico_pros_dt = {
            self.config['CONST']['SUBJECT_ID']: [],
            self.config['CONST']['HADM_ID']: [],
            self.config['CONST']['ICUSTAY_ID']: [],
            self.config['CONST']['HUNIT']: [],
            self.config['CONST']['PROCEDURE']: []
        }

        index = []
        for idx, (subject_id, hadm_id, icustay_id, unit, pros_dt) in enumerate(pros_by_window):
            dico_pros_dt[self.config['CONST']['SUBJECT_ID']].append(subject_id)
            dico_pros_dt[self.config['CONST']['HADM_ID']].append(hadm_id)
            dico_pros_dt[self.config['CONST']['ICUSTAY_ID']].append(icustay_id)
            dico_pros_dt[self.config['CONST']['HUNIT']].append(unit)
            # Convert tuple event, date to string
            dico_pros_dt[self.config['CONST']['PROCEDURE']].append(pros_dt)
            index.append(idx)

        return dico_pros_dt, index

    def get_pt_hamd_icus(self):
        """
            Get Patients based on list of SUBJECT ID
        """

        criteria = {}
        # criteria['nrows'] = 10

        return self.datareader.get_pt_hamd_icus(criteria)

    def get_outputevents(self):
        """
            Get CPTEVENTS by SUBJECT_ID AND HADM_ID
        """

        criteria = {}
        # criteria['nrows'] = 10

        return self.datareader.get_outputevents(criteria)

    def get_chartevents(self):
        """
            Get CPTEVENTS by SUBJECT_ID AND HADM_ID
        """

        criteria = {}
        # criteria['nrows'] = 10

        return self.datareader.get_chartevents(criteria)

    def merge_df(self, df_left, df_right, left, right, how, out_filename=None):
        """
            Merge dataframe
        """

        # Merge 2 tables Patients, Admissions and ICU Stays
        result = pd.merge(df_left, df_right, left_on=left, right_on=right, how=how)

        # Save Admissions to file
        if out_filename is not None:
            FileHelper.save_to_csv(result, out_filename)
        else:
            filename = self.config['OUT_DIR_S2'] + 'merged_df.csv'
            FileHelper.save_to_csv(result, filename)

        return result

    def __output_num_patient_by_limit(self, df_pt_adm_icu_outevs_charevs):

        if self.config['PARAM']['LIMIT_NUM_PATIENT'] > 0:
            num_patients = self.config['PARAM']['LIMIT_NUM_PATIENT']

            # Get unique subject_id from columns SUBJECT_ID
            list_unique_subject_id = df_pt_adm_icu_outevs_charevs['SUBJECT_ID'].unique().tolist()
            # Generate a uniform random sample from np.arange(len) of size num_patients
            ran_idx = np.random.choice(len(list_unique_subject_id), num_patients)
            # Get values of SUBJECT_ID using ran_idx and column name
            ran_subject_id = [df_pt_adm_icu_outevs_charevs['SUBJECT_ID'].iloc[idx] for idx in ran_idx]
            # Filter only matching subject_id in the list
            mask = df_pt_adm_icu_outevs_charevs['SUBJECT_ID'].isin(ran_subject_id)
            df_events_by_patient = df_pt_adm_icu_outevs_charevs[mask]

            # Save to File
            filename = self.config['OUT_DIR_S2'] + self.config['OUT_FNAME']['OUT_LIMIT_NUM_EVENTS_WINSIZE_24H']
            FileHelper.save_to_csv(df_events_by_patient, filename)


if __name__ == "__main__":

    # Input filename configuration
    F_INPUT = {
        'ADMISSIONS': 'ADMISSIONS.csv.gz',
        'PATIENTS': 'PATIENTS.csv.gz',
        'ICUSTAYS': 'ICUSTAYS.csv.gz',
        'OUTPUTEVENTS': 'OUTPUTEVENTS.csv.gz',
        'D_ITEMS': 'D_ITEMS.csv.gz',
        'CHARTEVENTS': 'CHARTEVENTS.csv',
        'CSV_OUT_PTS_ADMS_ICUS': 'OUT_PTS_ADMS_ICUS.csv',
        'CSV_OUT_OUTPUTEVENTS': 'OUT_OUTPUTEVENTS.csv',
        'CSV_OUT_CHARTEVENTS': 'OUT_CHARTEVENTS.csv'
    }

    # Output filename configuration
    F_OUTPUT = {
        'ADMISSIONS': 'OUT_ADMS.csv',
        'PATIENTS': 'OUT_PTS.csv',
        'PTS_ADMS': 'OUT_PTS_ADMS.csv',
        'ICUSTAYS': 'OUT_ICUSTAYS.csv',
        'OUTPUTEVENTS': 'OUT_OUTPUTEVENTS.csv',
        'PTS_ADMS_ICU': 'OUT_PTS_ADMS_ICUS.csv',
        'D_ITEMS': 'OUT_D_ITEMS.csv',
        'OUTPUTEVS_DITEMS': 'OUT_OUTPUTEVS_DITEMS.csv',
        'CHARTEVENTS': 'OUT_CHARTEVENTS.csv',
        'OE_BY_DATE_INTERVAL': 'OUT_OE_BY_DATE_INTERVAL.csv',
        'PT_ADM_ICU_OUTEVENT': 'OUT_PT_ADM_ICU_OUTEVENT.csv',
        'CHEV_BY_DATE_INTERVAL': 'OUT_CHEV_BY_DATE_INTERVAL.csv',
        'PT_ADM_ICUS_CHAREVS': 'OUT_PT_ADM_ICUS_CHAREVS.csv',
        'OUTEVENT_CHAREVS': 'OUT-OUTEVENT_CHAREVS.csv',
        'TEMP_DF': 'OUT_TEMP_DF.csv',
        'OUT_NUM_EVENTS_WINSIZE_24H':'OUT_NUM_EVENTS_WINSIZE_24H.csv',
        'OUT_LIMIT_NUM_EVENTS_WINSIZE_24H':'OUT_LIMIT_NUM_EVENTS_WINSIZE_24H.csv',
    }

    # CONST: THEY ARE USED IN STEP 2
    CONST = {
        'SUBJECT_ID': 'subject_id',
        'HADM_ID': 'hadm_id',
        'ICUSTAY_ID': 'icustay_id',
        'HUNIT': 'unit',
        'HUNIT_ICU': 'ICU',
        'HUNIT_CHAREV': 'CHAREV',
        'HUNIT_CHARTEVENT': 'CHEVENT',
        'PROCEDURE': 'procedure'
    }

    CONFIG = {
        'FILE_DIR': '/Volumes/SSD200/Users/Shared/Mimic/Data/Input/Step1/',
        'FILE_DIR_S2': '/Volumes/SSD200/Users/Shared/Mimic/Data/Input/Step2/',
        'OUT_DIR_S1': '/Volumes/SSD200/Users/Shared/Mimic/Data/Output/Step1/',
        'OUT_DIR_S2': '/Volumes/SSD200/Users/Shared/Mimic/Data/Output/Step2/',
        'IN_FNAME': F_INPUT,
        'OUT_FNAME': F_OUTPUT,
        'CONST': CONST,
        'NUM_ROWS': 100,
        'PREFIX_HADM': 'HADM_',
        'PREFIX_ICU': 'ICU_',
        'PREFIX_CHEV': 'CHEV_',
        'PREFIX_OUEV': 'OUEV_',
        'PREFIX_DITEM': 'DITEM_'
    }

    start = time.time()
    prepro = DataCompilation(**CONFIG)
    prepro.start_process()
    end = time.time()

    print("******** \nExecution time of records %d is %f \n******** \n" %
          (CONFIG['NUM_ROWS'], end - start))
