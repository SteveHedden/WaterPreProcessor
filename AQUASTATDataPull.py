#import requests
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import csv
import xlrd
import matplotlib.lines as mlines
import matplotlib.transforms as mtransforms
import xlsxwriter
import dask
import dask.dataframe as dd
import statsmodels.api as sm

data= pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\AQUASTAT.xlsx')

country_concordance=pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\CountryConcordance.xlsx')

data=pd.merge(data,country_concordance,how="left",left_on="Area",right_on="Area name")

series_concordance=pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\SeriesConcordance.xlsx')

#print (series_concordance.head())
#print(data.head())

data=pd.merge(data,series_concordance,how="left",left_on="Variable Name",right_on="Series name in Aquastat")
#print(data.head())
#print(data['Country Name in IFs'].unique)
data= data.drop(['Variable Id','Area Id','Symbol','Md'],axis=1)

data=data.dropna(how='any')
p= pd.pivot_table(data,index=['Country Name in IFs','Year'],columns=['Series name in IFs'],values=['Value'],aggfunc=[np.sum])

print(p.head())
writer= pd.ExcelWriter('AQUASTATForPull.xlsx',engine='xlsxwriter')
p.to_excel(writer,sheet_name='1',merge_cells=False)

writer.save()
#writer.close()
