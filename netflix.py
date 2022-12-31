import pandas as pd
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from df2gspread import df2gspread as df2
import requests 
from bs4 import BeautifulSoup
import re
import numpy as np


def log(command):
    tanggal2 = datetime.now()
    tgl = tanggal2.strftime('%Y-%m-%d %H:%M %p')
    text_log = '{0} {1} \n'.format(tgl, command)
    with open('your log file path.txt','a') as file:
        file.write(text_log)
        file.close()
    print(command)
        
def runtime(row):
    if row['Runtime_min']!='TBA' and row['Runtime_max']!='None':
        range_minmax = [*range(int(row['Runtime_min']),int(row['Runtime_max'])+1)]
        sum_temp = 0
        for x in range_minmax:
            sum_temp+=x
        avg_temp = sum_temp/int(len(range_minmax))
        return avg_temp
    elif row['Runtime_min']!='TBA' and row['Runtime_max']=='None':
        avg_temp= int(row['Runtime_min'])
        return avg_temp
    
def replace_row(dataframe,regex_str,column_target,str_replace,index,replace_regex=''):
    replace = re.sub(r'{0}'.format(regex_str),replace_regex,str(str_replace))
    index_loc = dataframe['index'] == index
    dataframe.loc[index_loc, column_target] = dataframe.loc[index_loc, column_target].replace(str_replace, str(replace))
    

def split_season_process(dataframe,regex_str,column_target,replace_regex=''):
    for column_season,column_index in zip(dataframe['Seasons'],dataframe['index']):
        if column_season != 'TBA':
            replace_row(dataframe=dataframe,regex_str=regex_str,column_target=column_target,str_replace=column_season,index=column_index,replace_regex=replace_regex)
        else:
            pass

try:
    main_folder = 'your main folder path'
    folder_output = main_folder+'output\\'
    folder_config = main_folder+'config\\'
    url_spreadsheet  = 'url_spreadsheet.txt'
    json_creden = 'your json credential'
    url_netflix = 'https://en.wikipedia.org/wiki/List_of_Netflix_original_programming'
    scope = ['https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive']
    today = (datetime.now()).strftime('%Y-%m-%d')
    with open(folder_config+url_spreadsheet, 'r') as file_1:
        url = file_1.read()
    list_tozero = ['Seasons','episodes','Runtime_min','Runtime_max']
    
    log('Load variable success')
except:
    log('Load variable failed')
    
try:
    df_os = pd.DataFrame(columns = ['Title','Genre','Premiere','Seasons','Runtime','Language','Status'])
    for x in range(0,25):
        req = requests.get(url_netflix)
        soup = BeautifulSoup(req.content, "html.parser")
        table = soup.find_all('table')[int(x)]
        df_temp=pd.read_html(str(table),flavor='html5lib')
        df_temp_fix=pd.DataFrame(df_temp[0])
        df_os = pd.concat([df_os,df_temp_fix])
    df_os = df_os.reset_index()
    log('Load dataframe success')
except:
    log('Load dataframe failed')
    

try:
    df_os = df_os[~(df_os['Title']=='0')]
    for x in df_os.columns.tolist():
        if x != 'index':
            for value_target,value_index in zip(df_os[x],df_os['index']):
                replace_row(dataframe=df_os,regex_str='\[\d+\]',column_target=x,str_replace=value_target,index=value_index)
        
    df_os.drop('index',axis=1,inplace=True)
    df_os['Seasons'] = df_os['Seasons'].str.replace(' ','') 
    df_os[['Seasons', 'episodes']] = df_os['Seasons'].str.split(',', 1, expand=True)
    df_os['episodes'] = df_os['episodes'].str.replace('episodes','')
    df_os['episodes'] = df_os['episodes'].str.replace('episode','')
    df_os = df_os[~(df_os['Title']=='Awaiting release')]
    log('Transform general success')
except:
    log('Transform general failed')
    

try:
    split_season = df_os[~((df_os['Seasons'].str.contains('season')) | (df_os['Seasons'].str.contains('Season')))]
    season = df_os[((df_os['Seasons'].str.contains('season')) | (df_os['Seasons'].str.contains('Season')))]
    split_season = split_season.reset_index()
    split_season_process(dataframe=split_season,regex_str='.*',column_target='Seasons',replace_regex='other')
    split_season.drop(['index'],axis=1,inplace=True)
    concat_original = pd.concat([season,split_season]).reset_index().drop('index',axis=1)
    concat_original['Seasons'] = concat_original['Seasons'].str.replace('seasons','')
    concat_original['Seasons'] = concat_original['Seasons'].str.replace('season','')
    log('Transform column season success')
except:
    log('Transform column season failed')
    
try:
    concat_original['Runtime'] = concat_original['Runtime'].str.replace(' ','')
    concat_original['Runtime'] = concat_original['Runtime'].str.replace('min.','')
    concat_original['Runtime'] = concat_original['Runtime'].str.replace('min','')
    concat_original[['Runtime','Runtime_max']]=concat_original['Runtime'].str.split('–', 1, expand=True)
    concat_original['Runtime_max'] = concat_original['Runtime_max'].astype('str')
    concat_original.rename(columns={'Runtime':'Runtime_min'},inplace=True)
    concat_original['avg_runtime'] = concat_original.apply(lambda row: runtime(row), axis=1)
    concat_original = concat_original[['Title','Genre','Premiere','Seasons','episodes','Runtime_min','Runtime_max','avg_runtime','Status']]
    log('Transform column runtime success')
except:
    log('Transform column runtime success')
    
try:
    list_tozero = ['Seasons','episodes','Runtime_min','Runtime_max']
    for column in list_tozero:
        concat_original[column] = concat_original[column].astype('str')
        concat_original[column] = concat_original[column].replace('TBA','0')
        concat_original[column] = concat_original[column].replace('otherother','0')
        concat_original[column] = concat_original[column].replace('None','0')
        concat_original[column] = concat_original[column].astype('int64')
    concat_original['avg_runtime'] = concat_original['avg_runtime'].fillna(0.0)
    concat_original['Premiere'] = pd.to_datetime(concat_original['Premiere'])
    log('Change datatype success')
except:
    log('Change datatype failed')
    
try:
    concat_original['coming_soon'] = np.where(concat_original['Premiere']>today,'True','False')
    concat_original['renew_season'] = np.where((concat_original['Status'].str.contains('Renew')) | (concat_original['Status'].str.contains('renew')),'True','False')
    concat_original['final_season'] = np.where((concat_original['Status'].str.contains('Final')) | (concat_original['Status'].str.contains('final')),'True','False')
    concat_original['year_release'] = concat_original['Premiere'].dt.strftime('%Y')
    coming_soon = concat_original[concat_original['coming_soon']=='True']
    without_comingsoon = concat_original[concat_original['coming_soon']=='False']
    log('Add extra column success and split dataframe success')
except:
    log('Add extra column and split dataframe failed')
    
try:
    credentials1 = ServiceAccountCredentials.from_json_keyfile_name(folder_config+json_creden, scope)
    df2.upload(concat_original, gfile=url, wks_name='original', credentials=credentials1, row_names=False, clean=True)
    df2.upload(coming_soon, gfile=url, wks_name='coming_soon', credentials=credentials1, row_names=False, clean=True)
    df2.upload(without_comingsoon, gfile=url, wks_name='without_coming_soon', credentials=credentials1, row_names=False, clean=True)
    log('Upload to spreadsheet success')
except:
    log('Upload to spreadsheet failed')
    



    
    

