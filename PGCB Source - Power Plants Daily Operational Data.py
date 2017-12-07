
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
from datetime import timedelta
from IPython.display import display
from pathlib import Path
import xlrd
# import mysql.connector
# from sqlalchemy import create_engine


# In[2]:


cwd = os.getcwd()
input_path = cwd + '/PGCBFiles/'
output_path = cwd + '/PGCBOut/'


# In[3]:


def match_strings(d, string):
    """Returns a tuple of row and column indices that identifies the position matching the passed string.

    Arguments:
    d -- a dictionary created from a pandas DataFrame
    string -- the substring to be matched
    """
    for k1,v1 in d.items():
        for k2,v2 in v1.items(): 
            if type(v2) == str and v2.find(string) != -1:
                mytuple = (k2,k1)
                return mytuple


# In[4]:


def get_all_index_pairs(d):
    """Returns a list of tuples. Each tuple holds the start and end indices of the corresponding region's power plants.

    Arguments:
    data -- the raw pandas DataFrame created from source excel file
    d -- a dictionary created from a pandas DataFrame
    """
    dhaka = (match_strings(d,'Name of')[0]+4 , match_strings(d,'Dhaka Area')[0])
    chittagong = (dhaka[1]+1,match_strings(d,'Chittagong  area')[0])
    comilla = (chittagong[1]+1,match_strings(d,'Comilla Area')[0])
    mymensingh = (comilla[1]+1,match_strings(d,'Mymensin')[0])
    sylhet = (mymensingh[1]+1,match_strings(d,'Sylhet Area')[0])
    khulna = (sylhet[1]+1,match_strings(d,'Khulna Area')[0])
    barisal = (khulna[1]+1,match_strings(d,'Barisal Area')[0])
    rajshahi = (barisal[1]+1,match_strings(d,'Rajshahi Area')[0])
    rangpur = (rajshahi[1]+1,match_strings(d,'Rangpur Area')[0])
    
    return [dhaka,chittagong,comilla,mymensingh,sylhet,khulna,barisal,rajshahi,rangpur]


# In[5]:


# df = pd.read_excel(input_path + 'Daily Report10-01-17.xls', sheet_name="forecast", header = None)
# df.head(30)


# In[6]:


column_names_probable = ["Name_of_Power_Station","Fuel_Type_of_Power_Station","Powerplant_Under","Probable_Peak_Day_MW","Probable_Peak_Evening_MW"]

column_names_actual = ["Name_of_Power_Station","Fuel_Type_of_Power_Station","Powerplant_Under",
                              "Installed_Capacity_MW", "Present_Capacity_MW", "Actual_Peak_Day_MW","Actual_Peak_Evening_MW",
                              "Gen_Shortfall_Gas_Water_Limitation_MW","Gen_Shortfall_Machines_Shutdown_MW",
                               "Description_of_Machines_Under_Shutdown"]


# In[7]:


regions = ["Dhaka","Chittagong","Comilla","Mymensingh","Sylhet","Khulna","Barisal","Rajshahi","Rangpur"]


# In[8]:


my_file = Path(output_path + 'probable.csv')
existingFiles = []
if my_file.is_file():
    probableDF = pd.read_csv(output_path + 'probable.csv')
    existingFiles = set(probableDF['File_Name'])
    actualDF = pd.read_csv(output_path + 'actual.csv')
else:
    actualDF = pd.DataFrame()
    probableDF = pd.DataFrame()

for file in os.listdir(input_path):
    # if there is a new file in the input file directory, then process the file and append the cleaned data to the DataFrame
    if file not in existingFiles:
        filename = input_path + str(file)
        df = pd.read_excel(filename, sheet_name='forecast', header=None).reset_index(drop=True)
        d = df.to_dict()
        date_tuple = match_strings(d, 'Date')
        index_list = get_all_index_pairs(d)
        csi = match_strings(d,'Name of')[1] # column start index; used for resolving the issue of the first column index varying across the raw DataFrames

        for i in range(len(index_list)):
            region = regions[i]
            row_index = index_list[i]
            df1 = df.iloc[row_index[0]:row_index[1],[csi,csi+2,csi+3,csi+9,csi+10]].copy()
            df2 = df.iloc[row_index[0]:row_index[1],[csi,csi+2,csi+3,csi+5,csi+6,csi+7,csi+8,csi+11,csi+12,csi+13]].copy()
            df2['Installed_Minus_Derated_MW'] = pd.to_numeric(df.iloc[row_index[0]:row_index[1],csi+5]) - pd.to_numeric(df.iloc[row_index[0]:row_index[1],csi+6])
            df2['Division'] = region
            df1['Division'] = region
            df1['Date'] = (df.iloc[date_tuple[0],date_tuple[1]+1]).strftime('%Y-%m-%d')
            df2['Date'] = (df.iloc[date_tuple[0],date_tuple[1]+1]-timedelta(days=1)).strftime('%Y-%m-%d')
            df1['File_Name'] = str(file)
            df1.columns = column_names_probable + ['Division'] + ["Date"] + ["File_Name"]
            df2.columns = column_names_actual + ['Installed_Minus_Derated_MW'] + ['Division'] + ['Date']
            df1 = df1[df1['Name_of_Power_Station'].notnull()]
            df2 = df2[df2['Name_of_Power_Station'].notnull()]
            df1 = df1.replace(np.nan, "", regex=True)
            df2 = df2.replace(np.nan, "", regex=True)
            probableDF = pd.concat([probableDF,df1],axis=0)
            actualDF = pd.concat([actualDF,df2],axis=0)


# In[9]:


probableDF.reset_index(drop=True, inplace=True)
actualDF.reset_index(drop=True, inplace=True)


# In[10]:


probableDF.head()


# In[11]:


actualDF.head()


# In[12]:


actualDF.to_csv(
    output_path + "actual.csv",
    columns=["Date"]+column_names_actual+['Installed_Minus_Derated_MW']+["Division"],
    index=None)
probableDF.to_csv(output_path + "probable.csv",
                  columns=["Date"]+column_names_probable+["Division"]+['File_Name'],
                  index=None)


# In[13]:


# import mysql.connector
# from sqlalchemy import create_engine


# In[18]:


# engine = create_engine('mysql+mysqlconnector://root:root@127.0.0.1/pdb', echo=False)
# probableDF[1:].to_sql(name='pdb_plants_probable', con=engine, if_exists = 'append', index=False)

