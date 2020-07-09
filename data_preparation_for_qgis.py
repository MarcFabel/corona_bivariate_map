#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  7 11:09:53 2020

@author: marcfabel
"""




# packages
import pandas as pd
import numpy as np
import time
start_time = time.time()



# work directories (LOCAL)
z_regional_source =           '/Users/marcfabel/econ/projects_twitter_job/map_infektionszahlen_rki/data/src/'
z_regional_output =           '/Users/marcfabel/econ/projects_twitter_job/map_infektionszahlen_rki/data/data_prepared/'
z_regional_output_qgis =      '/Users/marcfabel/econ/projects_twitter_job/map_infektionszahlen_rki/data/output/'
#z_maps_input_intermediate =   '/Users/marcfabel/econ/projects_twitter_job/'



election = pd.read_csv(z_regional_source +  'elections_eu_2019.csv', 
                       sep=';', encoding='ISO-8859-1', dtype=str, skiprows=8,
                       skipfooter=4)

election.columns=['date', 'AGS', 'AGS_Name', 'nr_elegible_votes', 'turnout', 'valid_votes',
                    'cdu_csu', 'spd', 'greens', 'fdp', 'linke', 'afd', 'others']

# drop irrelevent rows
z_delete_rows = election[election['AGS'] == 'DG'].index
election.drop(z_delete_rows, inplace=True)
election['year'] = pd.to_numeric(election.date.str.slice(-4,))


# replace hamburg and bremen and Berlin with full 
election.AGS = election.AGS.replace({'02': '02000',
                                     '11': '11000',})

election = election[election['AGS'].map(len) == 5]


#election['AGS'] = election['AGS'].str.ljust(8, fillchar='0')
election['turnout'] = election['turnout'].str.replace(',','.')
election['AGS'] = election['AGS'].astype(int)
#election = election.merge(active_regions, on='AGS')
election = election.apply(pd.to_numeric, errors='coerce')
# have vote shares per party
for party in ['cdu_csu', 'spd', 'greens', 'fdp', 'linke', 'afd', 'others']:
	election['elec_' + party] = (election[party] / election['valid_votes']) * 100
	election.drop(party, inplace=True, axis=1)
election.drop(['date', 'AGS_Name', 'nr_elegible_votes', 'valid_votes', 'year'], inplace=True, axis=1)


election.to_csv(z_regional_output + 'election_prepared.csv',
                sep=';', encoding='UTF-8', index=False, float_format='%.3f')




##### ARBEITSMARKT ############################################################
labor = pd.read_csv(z_regional_source +  'lab_2019.csv', 
                       sep=';', encoding='ISO-8859-1', dtype=str, skiprows=7,
                       skipfooter=4)
labor.columns=['year', 'AGS', 'AGS_Name', 'ue', '1', '2', '3', '4', '5', '6',
                    'ue_rate1', 'ue_rate2', '7', '8', '9', '10']

labor = labor[['AGS', 'ue', 'ue_rate1', 'ue_rate2']].copy()


labor.AGS = labor.AGS.replace({'02': '02000',
                                     '11': '11000',})

labor = labor[labor['AGS'].map(len) == 5]
labor['ue_rate1'] = labor['ue_rate1'].str.replace(',','.')
labor['ue_rate2'] = labor['ue_rate2'].str.replace(',','.')


labor.to_csv(z_regional_output + 'labor_prepared.csv',
                sep=';', encoding='UTF-8', index=False)


#### Population
pop = pd.read_csv(z_regional_source +  'pop_2018.csv', 
                       sep=';', encoding='ISO-8859-1', dtype=str, skiprows=0,
                       skipfooter=4)
pop.columns=['AGS', 'AGS_Name', 'pop_2018', '1', '2']
pop = pop[['AGS', 'pop_2018']].copy()
pop.AGS = pop.AGS.replace({'11': '11000',})
pop = pop[pop['AGS'].map(len) == 5]

pop.to_csv(z_regional_output + 'population_prepared.csv',
                sep=';', encoding='UTF-8', index=False)




biv_class = pd.read_csv(z_regional_output_qgis + 'map_output_bivariate_classification.csv',
                        sep=';')
biv_class.drop_duplicates(subset='AGS', inplace=True)
active_ags = biv_class[['AGS']].copy()




#### Look at hospital data
hospital =  pd.read_csv(z_regional_source +  'hosp_2017.csv', 
                       sep=';', encoding='ISO-8859-1', dtype=str, skiprows=7,
                       skipfooter=4)

hospital.columns=['year', 'AGS', 'AGS_Name', 'hospitals', 'beds', '2', '3', '4', '5', '6',
                    'ue_rate1', 'ue_rate2', '7', '8', '9', '10', '11', '12', '13', '14']
hospital = hospital[['AGS', 'hospitals', 'beds']].copy()

hospital.AGS = hospital.AGS.replace({'02': '02000',
                                     '11': '11000',})
hospital = hospital[hospital['AGS'].map(len) == 5]
hospital = hospital.apply(pd.to_numeric, errors='coerce')
hospital = hospital.merge(active_ags, on=['AGS'])



# Merge hospital data to biv_class
biv_class = biv_class.merge(hospital, on=['AGS'])



biv_class['beds_p100k'] = (biv_class['beds'] / biv_class['2018_pop']) * 100000


beds_density_per_grp = biv_class.groupby('Bi_Class')['beds_p100k'].agg(['mean', 'std', 'count'])






