# %load src/data/jh_data.py

import pandas as pd
import numpy as np

from datetime import datetime


def store_relational_JH_data():
    ''' Transformes the COVID data in a relational data set

    '''

    data_path='data/raw/COVID-19/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv'
    pd_raw=pd.read_csv(data_path)

    pd_data_base=pd_raw.rename(columns={'Country/Region':'country',
                      'Province/State':'state'})

    pd_data_base['state']=pd_data_base['state'].fillna('no')

    pd_data_base=pd_data_base.drop(['Lat','Long'],axis=1)
    pd_data_base_country=pd_data_base.groupby('country').agg(np.sum)
    pd_data_base_country=pd_data_base_country.T.stack(level=[0]).reset_index().rename(columns={'level_0':'date',0:'confirmed'})

    pd_data_base_country.confirmed=pd_data_base_country.confirmed.astype(int)

    pd_data_base_country.to_csv('data/processed/COVID_relational_database_by_country_confirmed.csv',sep=';',index=False)

    print(' Number of rows stored: '+str(pd_data_base_country.shape[0]))
    print(' Latest date is: '+str(max(pd_data_base_country.date)))
if __name__ == '__main__':

    store_relational_JH_data()
