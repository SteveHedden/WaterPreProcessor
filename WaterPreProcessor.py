import requests
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import csv
import xlrd
import matplotlib.lines as mlines
import matplotlib.transforms as mtransforms
import xlsxwriter
#import dask.dataframe as dd
import statsmodels.api as sm
from sklearn import linear_model
import seaborn as sns

dat= pd.read_csv('C:\\Users\Steve\PycharmProjects\SecondProject\AQUASTATForPull.csv')
dat['country'] = dat['Country Name in IFs']
dat['yearRef'] = dat['Year']
countryList = dat.drop_duplicates(['country'])
countryList = countryList[['Country Name in IFs']]
countryList['country'] = countryList[['Country Name in IFs']]
countryList.set_index(['Country Name in IFs'], inplace=True)
dataForModel = countryList
seriesNames = dat.columns.values.tolist()
seriesNames.remove('Year')
seriesNames.remove('country')
seriesNames.remove('Country Name in IFs')
seriesNames.remove('yearRef')
dat.set_index(['Country Name in IFs'], inplace=True)
DesalinatedWater = dat.loc[:, ['DesalinatedWater', 'yearRef', 'country']]
WaterWithdMunicipal = dat.loc[:,['WaterWithdMunicipal', 'yearRef', 'country']]

def getYrOrMstRct(lookback, dat, baseyear, datDest, SeriesName):
    for y in range(0, lookback, +1):
        year = baseyear - y
        print(year)
        Current = dat.loc[dat['yearRef'] == year, :]
        Current = Current.replace(np.nan, '', regex=True)
        for index, country in countryList.iterrows():
            country = country['country']
            if not datDest.loc[[country], [SeriesName]].values[0]:
                if country in Current.index:
                    datDest.loc[[country], [SeriesName]] = \
                        Current.loc[Current['country'] == country, [SeriesName]].values[0]
#Call getYrOrMstRct on all series

for x in seriesNames:
    print(x)
    dataForModel[x] = ""
    CurrentSeries = dat.loc[:, [x, 'yearRef', 'country']]
    getYrOrMstRct(10, CurrentSeries, 2015, dataForModel, x)

#Write to excel
writer= pd.ExcelWriter('AQUASTATForModel.xlsx',engine='xlsxwriter')
dataForModel.to_excel(writer,sheet_name='1',merge_cells=False)
writer.save()
writer= pd.ExcelWriter('AQUASTATForModel2.xlsx',engine='xlsxwriter')
dataForModel.to_excel(writer,sheet_name='1',merge_cells=False)
writer.save()


dat2= pd.read_excel('C:\\Users\Steve\PycharmProjects\SecondProject\AQUASTATForModel2.xlsx',sheet = '1')
dat2.set_index(['Country Name in IFs'], inplace=True)

for index, country in countryList.iterrows():
    country = country['country']
#Fill in agriculture water demand if we have total and both industrial and municipal
    if np.isnan(dat2.loc[[country], ['WaterWithdAgriculture']].values[0]):
        if not np.isnan(dat2.loc[[country], ['WaterTotalWithd']].values[0]) and \
                not np.isnan(dat2.loc[[country], ['WaterWithdIndustrial']].values[0]) and \
                    not np.isnan(dat2.loc[[country], ['WaterWithdMunicipal']].values[0]):
            dat2.loc[[country], ['WaterWithdAgriculture']] = \
                dat2.loc[[country],['WaterTotalWithd']].values[0] - \
                (dat2.loc[[country],['WaterWithdIndustrial']].values[0] + dat2.loc[[country],['WaterWithdMunicipal']].values[0])
            if dat2.loc[[country], ['WaterWithdAgriculture']].values[0] < 0:
                dat2.loc[[country], ['WaterWithdAgriculture']] = 0
#Fill in industrial water demand if we have total and both agriculture and municipal
    if np.isnan(dat2.loc[[country], ['WaterWithdIndustrial']].values[0]):
        if not np.isnan(dat2.loc[[country], ['WaterTotalWithd']].values[0]) and \
                not np.isnan(dat2.loc[[country], ['WaterWithdAgriculture']].values[0]) and \
                    not np.isnan(dat2.loc[[country], ['WaterWithdMunicipal']].values[0]):
            dat2.loc[[country], ['WaterWithdIndustrial']] = \
                dat2.loc[[country],['WaterTotalWithd']].values[0] - \
                (dat2.loc[[country],['WaterWithdAgriculture']].values[0] + dat2.loc[[country],['WaterWithdMunicipal']].values[0])
            if dat2.loc[[country], ['WaterWithdIndustrial']].values[0] < 0:
                dat2.loc[[country], ['WaterWithdIndustrial']] = 0
#Fill in municipal water demand if we have total and both agriculture and industrial
    if np.isnan(dat2.loc[[country], ['WaterWithdMunicipal']].values[0]):
        if not np.isnan(dat2.loc[[country], ['WaterTotalWithd']].values[0]) and \
                not np.isnan(dat2.loc[[country], ['WaterWithdAgriculture']].values[0]) and \
                    not np.isnan(dat2.loc[[country], ['WaterWithdIndustrial']].values[0]):
            dat2.loc[[country], ['WaterWithdMunicipal']] = \
                dat2.loc[[country],['WaterTotalWithd']].values[0] - \
                (dat2.loc[[country],['WaterWithdAgriculture']].values[0] + dat2.loc[[country],['WaterWithdIndustrial']].values[0])
            if dat2.loc[[country], ['WaterWithdMunicipal']].values[0] < 0:
                dat2.loc[[country], ['WaterWithdMunicipal']] = 0
#Estimate ag water demand with irrigated area if we have data
    if np.isnan(dat2.loc[[country], ['WaterWithdAgriculture']].values[0]):
        y = dat2['WaterWithdAgriculture']
        y = y[y<100]
        X = dat2['LandEquipIrActual']
        test = pd.concat([y, X], axis=1)
        test.dropna(inplace=True)
        y = test['WaterWithdAgriculture']
        X = test['LandEquipIrActual']
        X = sm.add_constant(X)
        model = sm.OLS(y,X, data=test, missing='drop')
        p = model.fit()
        #print(p.summary())
        predict = p.predict(X)
        LandEquip = dat2['LandEquipIrActual']
        LandEquip.dropna(inplace=True)
        LandEquip = sm.add_constant(LandEquip)
        LandEquip['predict'] = p.predict(LandEquip)
        if country in LandEquip.index:
            dat2.loc[[country], ['WaterWithdAgriculture']] = (LandEquip.loc[[country], ['predict']].values[0])
#This is where we need UrbanPop, UrbanPop(percent), GDPPCP, and WATSAFE to estimate Municipal
#This is where we need VADD(man) to estimate Industrial

#Normalize water demand so that they sum to total

#Initialize growth rates for surface, ground, and fossil water withdrawals

##Total water resources
#Fill in total renewable surface water if we have total and ground
    if np.isnan(dat2.loc[[country],['WaterResTotalRenewSurface']].values[0]):
        if not np.isnan(dat2.loc[[country], ['WaterGroundTotal']].values[0]) and \
                not np.isnan(dat2.loc[[country], ['WaterResTotalRenew']].values[0]):
            dat2.loc[[country], ['WaterResTotalRenewSurface']] = \
                (dat2.loc[[country], ['WaterResTotalRenew']].values[0] - dat2.loc[[country], ['WaterGroundTotal']].values[0])
#Fill in total renewable ground water if we have total and surface
    if np.isnan(dat2.loc[[country],['WaterGroundTotal']].values[0]):
        if not np.isnan(dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0]) and \
                not np.isnan(dat2.loc[[country], ['WaterResTotalRenew']].values[0]):
            dat2.loc[[country], ['WaterGroundTotal']] = \
                (dat2.loc[[country], ['WaterResTotalRenew']].values[0] - dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0])
#Fill in total renewable water resources (surface and ground) if we have both surface and ground
    if np.isnan(dat2.loc[[country],['WaterResTotalRenew']].values[0]):
        if not np.isnan(dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0]) and \
                not np.isnan(dat2.loc[[country], ['WaterGroundTotal']].values[0]):
            dat2.loc[[country], ['WaterResTotalRenew']] = \
                (dat2.loc[[country], ['WaterResTotalSurface']].values[0] + dat2.loc[[country], ['WaterGroundTotal']].values[0])

#Now, either we have no data for total, surface, or ground, or we only have data for one of them.

#If we have no data for any then we estimate total renewable water resources (TRWR) using land area

#If we have no data for any 3 of them
    #Estimate total using land area - need land data
    #Assume split 71 - 29 surface - ground

#If we only have data for total:
#If we only have data for surface:
#If we only have data for ground:
#Subtract overlap of surface and ground and reestimate surface and ground:

##Exploitable water resources
#If we have total and surface, estimate ground
#If we have total and ground, estimate surface
#If we have surface and ground, estimate total


#Write to excel
writer= pd.ExcelWriter('AQUASTATForModel3.xlsx',engine='xlsxwriter')
#dataForModel.to_excel(writer,sheet_name='1',merge_cells=False)
dat2.to_excel(writer,sheet_name='1',merge_cells=False)
writer.save()

