"""
    Preprocessing
"""

import time, math

import pandas as pd
import numpy as np

from preprocessing.patient import Patient
from preprocessing.admission import Admission
from preprocessing.icustay import ICUStay
from preprocessing.cpt_event import CPTEvent
from preprocessing.chartevent import ChartEvent
from preprocessing.outputevent import OutputEvent
from preprocessing.d_item import DItem

from preprocessing.step2.data_compilation import DataCompilation

from utils.file_helper import FileHelper

class Preprocessing:
    """
        Preprocessing
    """

    def __init__(self, **kwargs):
        self.config = kwargs
        self.data_compilation = DataCompilation(**self.config)

    def start_process(self):
        """ Read data from tables: PATIENTS, ADMISSIONS, CPTEVENT
        """

        ### Read data from admission based on PAINTENT ID (SUBJECT_ID)
        criteria = {}
        criteria[self.config['CONST']['N_ROWS']] = 10

        ### Read admissions groupby date and
        ### Choose admissions of the year during which contains biggest number of admissions
        df_adms = self.get_adms(criteria)

        ### Table: Patient
        ### Conditions: SUBJECT_ID
        ids = df_adms[self.config['PREFIX_HADM'] + 'SUBJECT_ID'].unique()
        df_patients = self.get_patients_by_hamd(ids)

        ### Merge Tables: PATIENTS AND ADMISSIONS
        ### Conditions:
        ###   SUBJECT_ID : Patient's ID
        ###   HADM_SUBJECT_ID: Patient's ID of table ADMISSIONS
        left = ['SUBJECT_ID']
        right = [self.config['PREFIX_HADM'] + 'SUBJECT_ID']

        ### Filename of Tables PATIENTS AND ADMISSIONS
        out_fname_pts_adms = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['PTS_ADMS']
        df_pts_adms = self.merge_df(df_patients, df_adms, left=left, right=right,\
            how='outer', out_filename=out_fname_pts_adms)

        ### Calculate Patient's Age
        df_pts_adms = self.__calculate_patient_age(df_patients, df_pts_adms)

        ### Table: ICUSTAYS
        ### Conditions:
        ###   SUBJECT_ID : Patient's ID
        ###   HADM_SUBJECT_ID: Patient's ID of table ADMISSIONS
        pt_ids = df_pts_adms[self.config['PREFIX_HADM'] + 'SUBJECT_ID']
        hadm_ids = df_pts_adms[self.config['PREFIX_HADM'] + 'HADM_ID']
        df_icustays = self.get_icustays_by_pthamd(pt_ids, hadm_ids)

        ### Merge Tables: PATIENTS, ADMISSIONS and ICUSTAYS
        ### Conditions:
        ###   SUBJECT_ID : Patient's ID
        ###   HADM_SUBJECT_ID: Patient's ID of table ADMISSIONS
        left = [self.config['PREFIX_HADM'] + 'SUBJECT_ID', self.config['PREFIX_HADM'] + 'HADM_ID']
        right = [self.config['PREFIX_ICU'] + 'SUBJECT_ID', self.config['PREFIX_ICU'] + 'HADM_ID']
        ### Filename of compiling tables: PATIENTS, ADMISSIONS, ICUSTAYS
        out_fname_pt_adm_icus = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['PTS_ADMS_ICU']
        df_pts_adms_icus = self.merge_df(df_pts_adms, df_icustays, left=left, right=right,\
            how='outer', out_filename=out_fname_pt_adm_icus)

        ### Table OUTPUTEVENTS
        ### Conditions: SUJECT_ID, HADM_ID, ICUSTAY_ID
        pt_ids = df_pts_adms_icus[self.config['PREFIX_HADM'] + 'SUBJECT_ID']
        hadm_ids = df_pts_adms_icus[self.config['PREFIX_HADM'] + 'HADM_ID']
        df_outputevs = self.get_outputevents_by_pthadmicu(pt_ids, hadm_ids)

        ### Table D_ITEMS
        ### Conditions: ITEMID
        item_ids = df_outputevs[self.config['PREFIX_OUEV'] + 'ITEMID']
        df_ditems = self.get_ditems_outevents_by_itemid(item_ids)

        ### Merge tables: OUTPUTEVENTS and D_ITEMS
        ### Conditions: ITEMID
        left = [self.config['PREFIX_OUEV'] + 'ITEMID']
        right = [self.config['PREFIX_DITEM'] + 'ITEMID']
        ### Filename of combination tables: PATIENTS, ADMISSIONS, ICUSTAYS
        out_fname_oditems = self.config['OUT_DIR_S1'] +\
            self.config['OUT_FNAME']['OUTPUTEVS_DITEMS']
        ### df_outputevs_ditems = self.merge_df(df_outputevs, df_ditems, left=left, right=right,\
        ###     how='outer', out_filename=out_fname_outputevs_ditems)
        _ = self.merge_df(df_outputevs, df_ditems, left=left, right=right,\
        how='outer', out_filename=out_fname_oditems)

        ### Table CHARTEVENTS:
        ### Conditions: SUJECT_ID, HADM_ID, ICUSTAY_ID
        pt_ids = df_pts_adms_icus[self.config['PREFIX_HADM'] + 'SUBJECT_ID']
        hadm_ids = df_pts_adms_icus[self.config['PREFIX_HADM'] + 'HADM_ID']
        #df_chartevs = self.get_chartevents_by_pthadmicu(pt_ids, hadm_ids)
        _ = self.get_chartevents_by_pthadmicu(pt_ids, hadm_ids)

        #################################################################
        ### Moving 3 files OUT_PTS_ADMS_ICUS, OUT_PTS_ADMS_ICUS, OUT_CHARTEVENTS
        self.movefile_ins1_to_outs2()

        ##### Start Data Compilation Step 2
        self.start_data_compilation()

    def __calculate_patient_age(self, df_patients, df_pts_adms):
        """ Calculate patient's age since it was obscured
        """

        col_pai_subject_id = self.config['PREFIX_HADM'] + 'SUBJECT_ID'
        col_pai_hadm_id = self.config['PREFIX_HADM'] + 'HADM_ID'
        col_dob_year = 'DOB_YEAR'
        col_hamd_admintime = self.config['PREFIX_HADM'] + 'ADMITTIME'

        for idx, row in df_patients.iterrows():
            pt_sub_id = row['SUBJECT_ID']

            # Get outevents matching SUBJECT_ID and HAMD_ID
            mask = (df_pts_adms[col_pai_subject_id] == pt_sub_id)
            df_output = df_pts_adms[mask]

             # Sort Admintime by non decreasing order
            df_output = df_output.sort_values(col_hamd_admintime, ascending=True)

            # Calculate the Age
            year_sec = 60.0 * 60.0 * 24.0 * 365.242
            pt_age = 0
            try:
                admin_time = pd.to_datetime(df_output.iloc[0][col_hamd_admintime])
                dod = pd.to_datetime(row['DOB'])
                age_sec = math.ceil((admin_time - dod)/pd.offsets.Second(1))
                pt_age = math.ceil(age_sec/year_sec)
            except ValueError:
                age_sec = 0
            except Exception:
                # Output unexpected Exceptions.
                pt_age = 0

            # DOB_YEAR
            df_pts_adms.loc[df_pts_adms.SUBJECT_ID == pt_sub_id, col_dob_year] = pt_age

        return df_pts_adms

    def __shape_num_patient_by_limit(self, df_adms):
        """ Limit number of patient based on settting in PARAM
        """

        if self.config['PARAM']['READ_ALL_RECORDS'] == self.config['CONST']['K_NO'] \
            and self.config['PARAM']['LIMIT_NUM_PATIENT'] > 0:
            num_patients = self.config['PARAM']['LIMIT_NUM_PATIENT']

            # Get unique subject_id from columns SUBJECT_ID
            list_unique_subject_id = df_adms[self.config['PREFIX_HADM'] +  \
                'SUBJECT_ID'].unique().tolist()
            # Generate a uniform random sample from np.arange(len) of size num_patients
            ran_idx = np.random.choice(len(list_unique_subject_id), num_patients)
            # Get values of SUBJECT_ID using ran_idx and column name
            ran_subject_id = [df_adms[self.config['PREFIX_HADM'] + 'SUBJECT_ID'].iloc[idx] for idx in ran_idx]
            # Filter only matching subject_id in the list
            mask = df_adms[self.config['PREFIX_HADM'] +  'SUBJECT_ID'].isin(ran_subject_id)
            df_adms = df_adms[mask]

        # Get number of Patients
        # Get unique subject_id from columns SUBJECT_ID
        list_unique_subject_id = df_adms[self.config['PREFIX_HADM'] +  \
            'SUBJECT_ID'].unique().tolist()
        # Set number of Patients
        self.config['PARAM']['LIMIT_NUM_PATIENT'] = len(list_unique_subject_id)

        return df_adms

    def get_adms(self, criteria=None):
        """ Read admissions groupby date and
        Choose admissions of the year during which contains biggest number of admission
        """
         ### criteria = {'nrows':10}
        admission = Admission(**self.config)

        ### Read admissions groupby date and
        ### Choose admissions of the year during which contains biggest number of admissions
        # df_adms = admission.get_admissions_by_year(criteria)
        df_adms = admission.get_admissions(criteria)
        ### Limit number of patients based on condition LIMIT_NUM_PATIENT
        df_adms = self.__shape_num_patient_by_limit(df_adms)
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['ADMISSIONS']
        FileHelper.save_to_csv(df_adms, filename)

        return df_adms

    def get_patients_by_hamd(self, ids):
        """
            Get Patients based on list of SUBJECT ID
        """

        patient = Patient(**self.config)
        df_pts = patient.get_patients_by_ids(ids)

        ### Save Patients to file
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['PATIENTS']
        FileHelper.save_to_csv(df_pts, filename)

        return df_pts

    def get_cptevents_by_phamd(self, subject_ids, hadm_ids):
        """
            Get CPTEVENTS by SUBJECT_ID AND HADM_ID
        """

        criteria = {}
        criteria['SUBJECT_ID'] = subject_ids
        criteria['HADM_ID'] = hadm_ids

        cptevent = CPTEvent(**self.config)
        df_cptevents = cptevent.get_cptevents_by_subject_hamd(criteria)

        ### Save ICUStays filtered by Patients and Admissions
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['ICUSTAYS']
        FileHelper.save_to_csv(df_cptevents, filename)

        return df_cptevents

    def get_icustays_by_pthamd(self, subject_ids, hadm_ids):
        """
            Get CPTEVENTS by SUBJECT_ID AND HADM_ID
        """

        ### Conditions
        criteria = {}
        criteria[self.config['PREFIX_ICU'] + 'SUBJECT_ID'] = subject_ids
        criteria[self.config['PREFIX_ICU'] + 'HADM_ID'] = hadm_ids

        icustay = ICUStay(**self.config)
        df_icustays = icustay.get_icustays_by_subject_hamd(criteria)

        ### Save ICUStays filtered by Patients and Admissions
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['ICUSTAYS']
        FileHelper.save_to_csv(df_icustays, filename)

        return df_icustays

    def get_chartevents_by_pthadmicu(self, subject_ids=None, hadm_ids=None, icustay_ids=None):
        """ Retrieve CHARTEVENTS matching the give hospital admission
        """

        ### Conditions
        criteria = {}
        if subject_ids is not None:
            criteria[self.config['PREFIX_CHEV'] + 'SUBJECT_ID'] = subject_ids
        if hadm_ids is not None:
            criteria[self.config['PREFIX_CHEV'] + 'HADM_ID'] = hadm_ids
        if icustay_ids is not None:
            criteria[self.config['PREFIX_CHEV'] + 'ICUSTAY_ID'] = icustay_ids

        ### Read only 100 000 rows
        if self.config['PARAM']['LIMIT_NUM_CHARTEVENTS'] > 0:
            criteria[self.config['CONST']['N_ROWS']] = self.config['PARAM']['LIMIT_NUM_CHARTEVENTS']

        chartevent = ChartEvent(**self.config)
        df_chartevs = chartevent.get_chartevents_by_phadmicu(criteria)

        ### Save chartevents to file
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['CHARTEVENTS']
        FileHelper.save_to_csv(df_chartevs, filename)

        return df_chartevs

    def get_outputevents_by_pthadmicu(self, subject_ids=None, hadm_ids=None, icustay_ids=None):
        """ Retrieve OUTPUTEVENTS matching the give hospital admission

        Pararmeters
        -----------
            subject_ids : the list of patient id
            hamd_ids : the list hospital admission stay id

        Return
        ------

        """

        ### Conditions
        criteria = {}
        if subject_ids is not None:
            criteria[self.config['PREFIX_OUEV'] + 'SUBJECT_ID'] = subject_ids
        if hadm_ids is not None:
            criteria[self.config['PREFIX_OUEV'] + 'HADM_ID'] = hadm_ids
        if icustay_ids is not None:
            criteria[self.config['PREFIX_OUEV'] + 'ICUSTAY_ID'] = icustay_ids

        ### Read only 100 000 rows
        # criteria['nrows'] = 4500000
        outputevs = OutputEvent(**self.config)
        df_outputevs = outputevs.get_outputevents_by_pthadmicu(criteria)

        ### Save OUTPUTEVENTS to file
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['OUTPUTEVENTS']
        FileHelper.save_to_csv(df_outputevs, filename)

        return df_outputevs

    def get_ditems_outevents_by_itemid(self, item_id):
        """ Retrieve D_ITEMS of outputevents by item_id
        """

        ### Conditions
        criteria = {}
        criteria[self.config['PREFIX_DITEM'] + 'ITEMID'] = item_id

        ### Read only 100 000 rows
        ### criteria['nrows'] = 4500000

        ditem = DItem(**self.config)
        df_ditems = ditem.get_ditems_outevents_by_itemid(criteria)

        ### Save Admissions to file
        filename = self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['D_ITEMS']
        FileHelper.save_to_csv(df_ditems, filename)

        return df_ditems

    def merge_df(self, df_left, df_right, left, right, how, out_filename=None):
        """ Merge dataframe
        """

        ### Merge 2 tables Patients, Admissions and ICU Stays
        result = pd.merge(df_left, df_right, left_on=left, right_on=right, how=how)

        ### Save Admissions to file
        if out_filename is not None:
            ### filename = self.config['OUT_DIR_S1'] + out_filename
            FileHelper.save_to_csv(result, out_filename)
        else:
            filename = self.config['OUT_DIR_S1'] + 'merged_df.csv'
            FileHelper.save_to_csv(result, filename)

        return result

    def movefile_ins1_to_outs2(self):
        """ Move files from Output Step 1 to Input Step 2
            Three files to move: OUT_PTS_ADMS_ICUS, OUT_PTS_ADMS_ICUS, OUT_CHARTEVENTS
        """

        files_to_move = []
        files_to_move.append(self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['PTS_ADMS_ICU'])
        files_to_move.append(self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['CHARTEVENTS'])
        files_to_move.append(self.config['OUT_DIR_S1'] + self.config['OUT_FNAME']['OUTPUTEVENTS'])

        move_to = []
        move_to.append(self.config['FILE_DIR_S2'] + self.config['IN_FNAME']['CSV_OUT_PTS_ADMS_ICUS'])
        move_to.append(self.config['FILE_DIR_S2'] + self.config['IN_FNAME']['CSV_OUT_CHARTEVENTS'])
        move_to.append(self.config['FILE_DIR_S2'] + self.config['IN_FNAME']['CSV_OUT_OUTPUTEVENTS'])

        ### Move from Output Step 1 to Input Step 2
        for idx, src_file in enumerate(files_to_move):
            FileHelper.move_file(src_file, move_to[idx])

    def start_data_compilation(self):
        """ Preprocesing step 2
        Combine the outputs in step 1 AND
        Generate number of events applied to each patient during ICU for window size of 24 hours
        """

        dc_start = time.time()
        self.data_compilation.start_process()
        dc_end = time.time()

        exe_time = dc_end - dc_start
        msg = '*** Step 2: Execution time of records {0} is {1}'.format(self.config['PARAM']['NUM_ROWS'], exe_time)
        print(msg)

    def execute(self):
        """ Execute the preprocessing here
        """

        start = time.time()
        print('\n=====================================================================')
        print('\n*** Step 1: Manipulate Admission, ICUStay, OutputEvent and Chartevent\n')
        self.start_process()
        end = time.time()

        print("\n*** Execution time of %d patient(s) is %f ********\n" %\
            (self.config['PARAM']['LIMIT_NUM_PATIENT'], end - start))

if __name__ == "__main__":

    ### Input filename configuration
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

    ### Output filename configuration
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

    ### CONST: THEY ARE USED IN STEP 2
    CONST = {
        'SUBJECT_ID': 'subject_id',
        'HADM_ID': 'hadm_id',
        'ICUSTAY_ID': 'icustay_id',
        'HUNIT': 'unit',
        'HUNIT_ICU': 'ICU',
        'HUNIT_CHAREV': 'CHAREV',
        'HUNIT_CHARTEVENT': 'CHEVENT',
        'PROCEDURE': 'procedure',
        'K_YES': 'YES',
        'K_NO': 'NO',
        'N_ROWS': 'N_ROWS'
    }

    ### PARAM: Parameters to tune so as to sharp the number of ouput records
    ### DOB is the date of birth of the given patient. Patients who are older than 89 years old 
    # at any time in the database have had their date of birth shifted to obscure 
    # their age and comply with HIPAA
    ### When Setting READ_ALL_RECORDS=YES, then LIMIT_NUM_PATIENT has no effect
    # - LIMIT_NUM_CHARTEVENTS: there are 330 million records in this CHARTEVENTS, so it is good
    # to limit to 10 million records for less time consuming
    PARAM = {
        'READ_ALL_RECORDS': 'YES',
        'LIMIT_NUM_PATIENT': 0,
        'LIMIT_NUM_CHARTEVENTS': 10000
    }

    CONFIG = {
        'FILE_DIR': '/Volumes/DATASSD/Mimic/Data/Input/Step1/',
        'FILE_DIR_S2': '/Volumes/DATASSD/Mimic/Data/Input/Step2/',
        'OUT_DIR_S1': '/Volumes/DATASSD/Mimic/Data/Output/Step1/',
        'OUT_DIR_S2': '/Volumes/DATASSD/Mimic/Data/Output/Step2/',
        'IN_FNAME': F_INPUT,
        'OUT_FNAME': F_OUTPUT,
        'CONST': CONST,
        'PARAM': PARAM,
        'PREFIX_HADM': 'HADM_',
        'PREFIX_ICU': 'ICU_',
        'PREFIX_CHEV': 'CHEV_',
        'PREFIX_OUEV': 'OUEV_',
        'PREFIX_DITEM': 'DITEM_'
    }

    ###
    prepro = Preprocessing(**CONFIG)
    prepro.execute()
