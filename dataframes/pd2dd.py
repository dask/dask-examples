#%% Change working directory from the workspace root to the ipynb file location. Turn this addition off with the DataScience.changeDirOnImportExport setting
import os
try:
	os.chdir(os.path.join(os.getcwd(), 'dataframes'))
	print(os.getcwd())
except:
	pass

#%% [markdown]
# # Intro to Dask
# This presentation highlights some of some gotcha's with Dask when converting  from Pandas
# * Please note that Dask is under development and thus there improvments all the time.  
# In some cases the documentation may be missleading since some of the documentation comes directly from Pandas but has not yet implemented in Dask
# please see dask documentation for updated API.

#%% [markdown]
# # What is Dask?
# * Flexible library for parallel computing in python 
# * `Spark` in the world of `Pandas` (however still in development)

#%%
from dask.distributed import Client
# client = Client(n_workers=1, threads_per_worker=4, processes=False, memory_limit='2GB')
client =Client()
client

#%%
# create 2 dataFrames: 1. for Dask 2. for Pandas
import dask
import pandas as pd
ddf = dask.datasets.timeseries()
pdf = ddf.compute()
ddf.head()

#%%
# Dask does not update - thus no "inplace=True":
# e.g. rename, reset_index, dropna, 
print(pdf.columns)
pdf.rename(columns={'id':'ID','name':'Name','x':'coor_x', 'y':'coor_y'},inplace=True)
pdf.columns
#%%
# in Dask
print(ddf.columns)
ddf.columns = ['ID','Name','coor_x','coor_y']
ddf.columns

#%%
# RESET INDEX
pdf.reset_index(drop=True, inplace=True)
pdf.head()
#%%
ddf = ddf.reset_index()
ddf = ddf.drop(labels='timestamp', axis=1)
ddf.head()


#%%
# save files - preferable Parquet
# if to flat file - Dask saves mulitple partitions to a directory 
pdf.to_csv('~/tmp/df_sample.csv')
!ls ~/tmp/
#%%
!mkdir ~/tmp/csv_dir2}
ddf.to_csv('../../tmp/csv_dir2/ddf*.csv')
!ls ~/tmp/csv_dir2
# to fild number of partitions use dask.dataframe.npartitions



#%%
# pd.apply should have `meta` clause
# pdf.assign(Date=pdf.index(lambda x: pd.to_datetime.Date(x)))
pdf.assign(Date=pd.to_datetime(pdf.index).date)

ddf.assign(Date=pd.to_datetime(ddf.index).date)
ddf = 




#%%
import dask.dataframe as dd
def create_edge_attribute(df:dd.DataFrame)->dd.DataFrame:
    def set_list_att(x:pd.Series, att_col:list):
        l=[]
        for col in att_col:
            l.extend(x[col].values)
        return list(set(l))
    
    col_att =[col for col in df.columns if not col.startswith(('Node','len'))]
    gp_col = ([[gp, list(items)] for gp, items in groupby(sorted(col_att), key=lambda x: re.split(r'_.$',x)[0])])
    df_gp = df.groupby(df.index)
    list_df_gp = ([df_gp[att_col_gr].apply(set_list_att, att_col_gr
                                           , meta = pd.Series(dtype='object', name=f'{col_name}_att'))
                                    .repartition(npartitions=10)
                           for col_name, att_col_gr in gp_col])
    weight = df_gp.size().rename('weight').repartition(npartitions=10)
    list_df_gp.append(weight)
    return list_df_gp


#%%
def create_edges(df:dd.DataFrame, edgeID:str='suff', nodeID:str='author_date')->dd.DataFrame:
    nodeIDx=f'{nodeID}_x'
    nodeIDy=f'{nodeID}_y'    
    _query= f'{nodeIDx} > {nodeIDy}'
    df =(df.merge(df, how='inner', right_index=True, left_index=True)
            .query(_query)
            .rename(columns={nodeIDx:'Node1', nodeIDy:'Node2'}))
    return df


#%%
from datetime import timedelta
import random
df_raw = dask.datasets.timeseries()
df = df_raw[['id', 'name']]
df = df.assign(timegroup=df.index)
df.timegroup = df.timegroup.apply(lambda s: s + timedelta(seconds=random.randint(0,60)) ) 
df.head()


#%%
import pandas as pd
from typing import List
from itertools import groupby
import re
def create_edge_attributes(df: dd.DataFrame, col_att) -> dd.DataFrame:
    def set_list_att(x: pd.Series, att_col_gr: List):
        return list(set([item for col in att_col_gr for item in x[col].values]))

#     col_att = [col for col in df.columns if not col.startswith(('Node', 'len'))]
    gp_col = ([[gp, list(items)]
               for gp, items in groupby(sorted(col_att), key=lambda x: re.split(r'_.$', x)[0])])

    df_gb = df.groupby(df.index)
    df_edge_att = df_gb.size().to_frame(name="Weight")
    df_edge_att = df_edge_att[df_edge_att.Weight>1]
    list_ser_gb = [df_gb[att_col_gr].apply(set_list_att, att_col_gr,
                                           meta=pd.Series(dtype='object', name=f'{col_name}_att'))
                   for col_name, att_col_gr in gp_col]
    for ser in list_ser_gb:
        df_edge_att = df_edge_att.join(ser.to_frame(), how='left')
    return df_edge_att


#%%
# df = df.set_index('id')
df = client.persist(df)
df.head()


#%%
col_att=['name', 'timegroup']
gp_col = ([[gp, list(items)]
               for gp, items in groupby(sorted(col_att), key=lambda x: re.split(r'_.$', x)[0])])
gp_col


#%%
df_gb = df.groupby(df.index)
df_edge_att = df_gb.size().to_frame(name="Weight")
df_edge_att = df_edge_att[df_edge_att.Weight>1]


#%%
get_ipython().run_cell_magic('time', '', 'groups = df.groupby(df.index)\ndf_edge_att = df_gb.size().to_frame(name="Weight")\ndf_edge_att = df_edge_att[df_edge_att.Weight>1]\nprint(groups.aggregate(\'count\').head())')


#%%
get_ipython().run_cell_magic('time', '', "# df.groupby(df.index).agg(lambda x: list(x)).compute()\ndfc = create_edge_attributes(df, col_att=['name', 'timegroup'])\nprint(dfc.head())")


#%%
dfc.head()


#%%
df.groupby(df.index).count().compute()


#%%



#%%
dfe = create_edges(df, edgeID='id', nodeID='name'  )
dfe.head()

#%% [markdown]
# # Working with environments
# ## conda-forge

#%%



