# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 15:11:59 2022

@author: nedwards1
"""

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import easygui
import numpy as np
from datetime import date
from shutil import copyfile

# msg boxes requesting credentials
snowflake_user = easygui.enterbox("Enter your Snowflake User ID")
snowflake_pw = easygui.enterbox("Enter your Snowflake password")

# creates Snowflake connection
cnn = snowflake.connector.connect(
    user = snowflake_user, 
    password = snowflake_pw,
    account = 'tacoma',
    warehouse = 'ANALYSTS_WH',
    database = 'PWR',
    role = 'CREATOR',
    schema = 'Strategy'
    )


# copy Planner data download file to DataArchive
today = date.today()
download = "//fs109/pss/Performance Mgmt/Projects/Environmental & Compliance Metrics/DataUpdate/Environmental Compliance Planner DL.xlsx"
archive = f"//fs109/pss/Performance Mgmt/Projects/Environmental & Compliance Metrics/DataArchive/Environmental Compliance Planner DL {today}.xlsx"
copyfile(download, archive)

# read Planner data download file and transform data 
read_file = pd.read_excel(r'\\fs109\pss\Performance Mgmt\Projects\Environmental & Compliance Metrics\DataUpdate\Environmental Compliance Planner DL.xlsx')
df = read_file
df = df.reset_index(drop=True)
# replace spaces in column names with underscores
df.columns = df.columns.str.replace(' ', '_')
# add column indicating unavoidable spills
df["Unavoidable_Spill"] = np.where(df['Labels'].str.contains('UNAVOIDABLE SPILL'), 1, 0)
# add column indicating avoidable spills
df["Avoidable_Spill"] = np.where(((df['Labels'].str.contains('AVOIDABLE SPILL')) & (df['Unavoidable_Spill']==0)), 1, 0)
# add column indicating unavoidable spills
df["Violation"] = np.where(df['Labels'].str.contains('VIOLATION', na=False), 1, 0)
# add TPU Campus indicator column to handle more complication location scenarios 
df["TPU_Campus"] = np.where(df['Labels'].str.contains('TPU Campus'), 1, 0)
# add utility division column 
df["Utility_Division"] = np.where(df['Labels'].str.contains('Rail'), 'Rail', 
                                  np.where(df['Labels'].str.contains('Tacoma Water'), 'Tacoma Water',
                                           np.where(df['Labels'].str.contains('Tacoma Power - T&D'), 'Tacoma Power - T&D', 
                                                    np.where(df['Labels'].str.contains('Tacoma Power - Generation'), 'Tacoma Power - Generation', ''))))
# add location column 
df["Location"] = np.where(df['Labels'].str.contains('Headworks', na=False), 'Headworks',
                          np.where(df['Labels'].str.contains('McMillin Reservoir', na=False), 'McMillin Reservoir',
                                   np.where(df['Labels'].str.contains('T&D Wire', na=False), 'T&D Wire',
                                            np.where(df['Labels'].str.contains('T&D Line', na=False), 'T&D Line',
                                                     np.where(df['Labels'].str.contains('Nisqually', na=False), 'Nisqually',
                                                              np.where(df['Labels'].str.contains('Cowlitz', na=False), 'Cowlitz',
                                                                       np.where(df['Labels'].str.contains('Cushman', na=False), 'Cushman',
                                                                                np.where(df['Labels'].str.contains('Wynoochee', na=False), 'Wynoochee',
                                                                                         np.where(df['Labels'].str.contains('Alder', na=False), 'Alder',
                                                                                                      np.where(df['Labels'].str.contains('Taidnapam', na=False), 'Taidnapam',
                                                                                                               np.where(df['Labels'].str.contains('Mossyrock', na=False), 'Mossyrock',
                                                                                                                        np.where(df['Labels'].str.contains('Mayfield', na=False), 'Mayfield',
                                                                                                                                 np.where(df['Labels'].str.contains('Loveland - South Service Center', na=False), 'Loveland - South Service Center',
                                                                                                                                          np.where(df['Labels'].str.contains('Rail', na=False), 'Rail', ''))))))))))))))
# update location column to TPU Campus where Location is null and Utility_Division is NOT null and TPU_Campus is 1
df['Update_Loc'] = np.where(((df['Location'] == '') & (df['Utility_Division'] != '') & (df['TPU_Campus'] ==1)), 1, 0)
df['Location'] = np.where((df['Update_Loc'] ==1), 'TPU Campus', df['Location'])
# clean up columns: remove Task_ID, Description, Checklist_Items, Update_Loc, TPU_Campus
df = df.drop(['Task_ID', 'Description', 'Checklist_Items', 'Update_Loc', 'TPU_Campus'], axis =1)

# truncate the spills and violations tables in Snowflake prior to writing updated date there
trunc_spills = 'TRUNCATE env_comp_spills;'
trunc_violations = 'TRUNCATE env_comp_violations;'

cursor = cnn.cursor()
cursor.execute(trunc_spills)
cursor.execute(trunc_violations)

# create df  of spills and releases and truncate/write to Snowflake table PWR.STRATEGY.ENV_COMP_SPILLS
# create df of violations, remove unnecessary columns and truncate/write to Snowflake table PWR.STRATEGY.ENV_COMP_VIOLATIONS
spills=df[df['Bucket_Name'] == 'Spills and Releases']
spills.columns = spills.columns.str.lower()

violations = df[df['Violation'] == 1]
violations = violations.drop(['Unavoidable_Spill', 'Avoidable_Spill'], axis=1)
violations.columns = violations.columns.str.lower()

success, nchunks, nrows, _ =write_pandas(cnn, spills, 'env_comp_spills', quote_identifiers=False )
print('spills ' + str(success)+ ', ' + str(nchunks) + ', ' + str(nrows))

success, nchunks, nrows, _ = write_pandas(cnn, violations, 'env_comp_violations', quote_identifiers = False)
print('violations ' + str(success) + ', ' + str(nchunks) + ', ' + str(nrows))

# closes Snowflake connection
cnn.close()

print('done')