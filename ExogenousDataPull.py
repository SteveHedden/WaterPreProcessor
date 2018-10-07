import numpy as np
import pandas as pd

data= pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\EXOGENOUSVARIABLESFORWATERMODEL.xlsx')

country_concordance=pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\CountryConcordance.xlsx')

data=pd.merge(data,country_concordance,how="left",left_on="country",right_on="Exogenous country name")

#p= pd.pivot_table(data,index=['Country Name in IFs','year'],columns=['variable'],values=['value'],aggfunc=[np.sum])
p= pd.pivot_table(data,index=['Country Name in IFs','year'],columns=['variable'],values=['value'])
#series_concordance=pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\SeriesConcordance.xlsx')

writer= pd.ExcelWriter('ExogenousForModel.xlsx',engine='xlsxwriter')
p.to_excel(writer,sheet_name='1',merge_cells=False)

writer.save()