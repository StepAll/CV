import os
import io
import time
import datetime
import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from PIL import Image

import httplib2
import apiclient.discovery
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaIoBaseDownload

import streamlit as st

PATH = ""

PHOTO_FILE_ID =  st.secrets['PHOTO_FILE_ID']
DEMO1_IMG_FILE_ID = st.secrets['DEMO1_IMG_FILE_ID']
DEMO2_IMG_FILE_ID = st.secrets['DEMO2_IMG_FILE_ID']
MS_PL300_IMG_FILE_ID = st.secrets['MS_PL300_IMG_FILE_ID']
GOOGLESHEET_ID = st.secrets['GOOGLESHEET_ID'] # gs_id
SERVICE_ACCOUNT_FILE = st.secrets['SERVICE_ACCOUNT_FILE']


# google spreadsheets
F_CV_PAGE_ID = 0  # gs_page_id
F_CV_PAGE_NAME = 'f_cv' # gs_page_name
F_TOOLS_PAGE_ID = 1211272847 # gs_page_id
F_TOOLS_PAGE_NAME = 'f_tools' # gs_page_name


def get_google_service(service_account_file, api='sheets'):
    """return connection to google api
    api='sheets'
    api='drive'
    """
    service_account_file_json = json.loads(service_account_file, strict=False)
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(service_account_file_json, ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive'])
    httpAuth = credentials.authorize(httplib2.Http())
    if api == 'sheets':
        return apiclient.discovery.build('sheets', 'v4', http = httpAuth)
    elif api == 'drive':
        return apiclient.discovery.build('drive', 'v3', credentials=credentials)
    return None

def get_gs_table(service, gs_id, gs_page_name):
    """ Get Google Sheet's page"""
    result = service.spreadsheets().values().batchGet(spreadsheetId=gs_id, ranges=gs_page_name).execute()
    columns = result['valueRanges'][0]['values'][0]
    data = result['valueRanges'][0]['values'][1:]
    df = pd.DataFrame(data=data, columns=columns)
    return df

def get_cv2():
    # get CV data
    f_cv_parquet_file_name = f'{PATH}f_cv.to_parquet'
    f_tools_parquet_file_name = f'{PATH}f_tools.to_parquet'

    # f_cv
    # if parquet file doesn't exist or it is old get from gs and write to parquet
    if (        not os.path.isfile(f_cv_parquet_file_name) 
            or  datetime.datetime.fromtimestamp(os.path.getmtime(f_cv_parquet_file_name)).date() != datetime.datetime.now().date()
        ):
        with get_google_service(SERVICE_ACCOUNT_FILE, api='sheets') as service:
            f_cv = get_gs_table(service, GOOGLESHEET_ID, F_CV_PAGE_NAME)
        f_cv.to_parquet(f_cv_parquet_file_name)

    # f_tools
    if (        not os.path.isfile(f_tools_parquet_file_name) 
            or  datetime.datetime.fromtimestamp(os.path.getmtime(f_tools_parquet_file_name)).date() != datetime.datetime.now().date()
        ):
        with get_google_service(SERVICE_ACCOUNT_FILE, api='sheets') as service:
            f_cv = get_gs_table(service, GOOGLESHEET_ID, F_TOOLS_PAGE_NAME)
        f_cv.to_parquet(f_tools_parquet_file_name)


    f_cv = pd.read_parquet(f_cv_parquet_file_name)
    f_tools = pd.read_parquet(f_tools_parquet_file_name)

    f_cv['начало'] = pd.to_datetime(f_cv['начало'], format='%d.%m.%Y')
    f_cv['конец'] = pd.to_datetime(f_cv['конец'], format='%d.%m.%Y')
    f_cv['период'] =  f_cv['конец'] - f_cv['начало']
    f_cv['период'] = f_cv['период'].dt.days

    f_tools['вес'] = f_tools['вес'].map(lambda x: x.replace(',','.')).astype(float) 
    f_tools['Экспертиза'] = f_tools['Экспертиза'].astype(float)
    f_tools['экспертиза-вес'] = f_tools['вес'] * f_tools['Экспертиза']

    return f_cv, f_tools

def get_photo(photo_file_name, photo_file_id, width=None):
    if (        not os.path.isfile(photo_file_name) 
            or  datetime.datetime.fromtimestamp(os.path.getmtime(photo_file_name)).date() != datetime.datetime.now().date()
        ):
        service_google_drive = get_google_service(SERVICE_ACCOUNT_FILE, api='drive')
        request = service_google_drive.files().get_media(fileId=photo_file_id)
        fh = io.FileIO(photo_file_name, 'wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()

    photo = Image.open(photo_file_name)
    if width:
        w, h = photo.size
        ratio = h/w
        photo = photo.resize((int(width), int(width*ratio)))
    return photo

def days_to_year_month(days):
    year_nums = {   1:'год',
                    2:'годa',
                    3:'годa',
                    4:'годa'
    }

    month_nums = {  1:'месяц',
                    2:'месяца',
                    3:'месяца',
                    4:'месяца'
    }

    y = days//365
    m = days%365//30

    y_txt = year_nums.get(int(str(y)[-1]), 'лет') if int(str(y)[-2:]) < 5 or int(str(y)[-2:]) > 20 else 'лет'
    yy = f"{str(y)} {y_txt}" if y > 0 else ""
    m_txt = month_nums.get(int(str(m)[-1]), 'месяцев') if int(str(y)[-2:]) < 5 or int(str(y)[-2:]) > 20 else 'месяцев'
    mm = f"{str(m)} {m_txt}" if m > 0 else ""
    return f'{yy} {mm}'





f_cv, f_tools = get_cv2()

f_tools_agg = f_tools.groupby('группа1').agg({'экспертиза-вес': 'sum', 'вес':'sum' })
f_tools_agg['ср.экспертиза'] = f_tools_agg['экспертиза-вес'].div( f_tools_agg['вес']).astype(int)
f_tools_agg_list = f_tools_agg.sort_values('ср.экспертиза', ascending=False).index


# streamlit 
# st.set_page_config(page_title='CV',layout='wide')

# photo | contacts
photo_file_name = f'{PATH}photo_ava.jpg'
photo_file_id = PHOTO_FILE_ID
photo = get_photo(photo_file_name, photo_file_id, width=180)

ms_pl300_file_name = f'{PATH}MS_PL300.png'
ms_pl300_file_id = MS_PL300_IMG_FILE_ID
ms_pl300 = get_photo(ms_pl300_file_name, ms_pl300_file_id, width=180)

col1, col2, col3 = st.columns([2,5,2])
with col1:
    st.image(photo)
with col2:
    st.title("Олег Степанов")
    st.subheader("аналитик")
    st.write ("+7 927-223-9884")
    st.write ("solegn@gmail.com")
with col3:
    st.write ("[Резюме на hh.ru](https://saratov.hh.ru/applicant/resumes/view?resume=e2430864ff0148b2630039ed1f4d693363424a)")
    st.write ("[Резюме в PBI](https://app.powerbi.com/view?r=eyJrIjoiZDA1MGM0NzctZTQ0NS00MDY3LTkyNDgtYWVkZjFiOWM3MzEwIiwidCI6IjFmNWMwYzQ5LWU3OTQtNDQ4Ni1iNzU1LTIzOGRiNmMzZTMxZSJ9)")
    



# экспертиза
st.header("Экспертиза и опыт")
col1, col2 = st.columns([1,6])
with col1:
    st.image(ms_pl300,width=80)
with col2:   
    st.write ("Знания в области Power BI подтверждены сертификатом  \n[Microsoft: Power BI Data Analyst Associate](https://www.credly.com/badges/81d02bc4-c3a3-473c-a323-4ab82cf95fcf/public_url)")

plot_data = f_tools_agg.sort_values('ср.экспертиза', ascending=False)
fig = plt.figure(figsize=(6, 4))
ax = sns.barplot(data=plot_data, 
                y=plot_data.index, 
                x='ср.экспертиза', 
                orient='h', 
                color='#195B7F')
ax.set_xlim(0,100)
ax.set_xlabel(None)
ax.set_ylabel(None)
ax.set_xticklabels([])
ax.set_yticklabels(plot_data.index, color='gray', fontsize=12)
ax.tick_params(bottom=False, left=False)  # remove the ticks
sns.despine(bottom=True, left=True) # remove frame
for i, (p, pr) in enumerate(zip(plot_data.index, plot_data['ср.экспертиза'])):
    plt.text(s=p, x=1, y=i, color="#9CB5C7", verticalalignment="center", size=8)
plt.axis("off")
st.pyplot(fig)


tool_chosen = st.selectbox('Выбирайте категорию, чтобы посмотреь подробнее', tuple(f_tools_agg_list))
plot_data_filtered = f_tools[f_tools['группа1'] == tool_chosen].sort_values('Экспертиза', ascending=False)
fig = plt.figure(figsize=(3, 2))
ax = sns.barplot(data=plot_data_filtered, 
                y='инструмент', 
                x='Экспертиза', 
                orient='h', 
                color='#195B7F')
ax.set_xlim(0,100)
ax.set_xlabel(None)
ax.set_ylabel(None)
ax.set_xticklabels([])
ax.set_yticklabels(plot_data_filtered['инструмент'], color='gray', fontsize=12)
ax.tick_params(bottom=False, left=False)  # remove the ticks
sns.despine(bottom=True, left=True) # remove frame
for i, (p, pr) in enumerate(zip(plot_data_filtered['инструмент'], plot_data_filtered['Экспертиза'])):
    plt.text(s=p, x=1, y=i, color="#9CB5C7", verticalalignment="center", size=8)
plt.axis("off")

col1,col2 = st.columns([2,3])
with col1:
    st.pyplot(fig)
with col2:
    comm = plot_data_filtered.apply(lambda x: f"__{x['инструмент']}__:  {x['комментарий']}", axis=1).to_list()
    st.write('  \n'.join(comm))

# демо проекты

st.header("Демо проекты")
demo1_file_name = f'{PATH}demo1.jpg'
demo1_id = DEMO1_IMG_FILE_ID
demo1 = get_photo(demo1_file_name, demo1_id, width=160)

demo2_file_name = f'{PATH}demo2.jpg'
demo2_id = DEMO2_IMG_FILE_ID
demo2 = get_photo(demo2_file_name, demo2_id, width=160)

col1, col2 = st.columns([1,3])
with col1:
    st.image(demo1)
with col2:
    st.write('__PBI__: [Отчет о посещаемости блога PowerBI Russia](https://app.powerbi.com/view?r=eyJrIjoiZGQyMjU2MDQtNmM3MC00N2JmLWI2NWQtMjkwOWE3MmUwNGUyIiwidCI6IjFmNWMwYzQ5LWU3OTQtNDQ4Ni1iNzU1LTIzOGRiNmMzZTMxZSJ9)')

col1, col2 = st.columns([1,3])
with col1:
    st.image(demo2)
with col2:
    st.write('__PBI__: [Отчет о затратах на строительство каркасного дома](https://app.powerbi.com/view?r=eyJrIjoiMWZiMGQ5YzMtY2YyZi00MTRkLTkxZTUtYjYxZjk2MmYxOWQzIiwidCI6IjFmNWMwYzQ5LWU3OTQtNDQ4Ni1iNzU1LTIzOGRiNmMzZTMxZSJ9)')


# история
st.header("История")
st_cv = f_cv[['начало', 'компания', 'должность', 'суть работы', 'достижения']].sort_values('начало',ascending=False).copy()
st_cv['Работа'] = st_cv.apply(lambda x: f"{x['суть работы']}  \n{x['достижения']}", axis=1)
st_cv = st_cv[['начало', 'компания', 'должность', 'Работа']]
st_cv.columns = ['Старт', 'Компания', 'Позиция', 'Работа'] 
st_cv['Старт'] = st_cv['Старт'].map(lambda x: str(x.strftime('%d.%m.%Y')))
st.table(st_cv[0:1])
with st.expander("Посмотреть всю историю"):
    st.table(st_cv[1:])

# доп. информация
st.header("Дополнительная информация")
st.write('* Уровень знания _английского языка_ достаточный для получения самой последней информации по профессиональной теме из первоисточников  \n* С 2022 года оказываю консультационные услуги по Power BI и DAX')



