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
import math

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
'''
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
'''

dat2= pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\AQUASTATForModel2.xlsx',sheet = '1')
dat2.set_index(['Country Name in IFs'], inplace=True)

Exog = pd.read_excel('C:\\Users\Steve\PycharmProjects\WaterPreProcessor\ExogenousForModel.xlsx',sheet = '1')
Exog.set_index(['Country Name in IFs'], inplace=True)
Exog = Exog.loc[Exog['year'] == 2015, :]
Exog = Exog.rename(columns={'value.LANDIRAREAACTUAL[1]': 'LANDIRAREAEQUIPACTUAL', 'value.GDPPCP[1]': 'GDPPCP', \
                            'value.LANDAREA[1]':'LANDAREA', 'value.POPURBAN/POP':'UrbanPercent', \
                            'value.POPURBAN[1]':'POPURBAN', 'value.VADD[1]':'VADD(man)', \
                            'value.WATSAFE[1]':'WATSAFE(piped)'})
Exog['logUrbanPercent'] = np.log(Exog['UrbanPercent'])
Exog['logWATSAFE(piped)'] = np.log(Exog['WATSAFE(piped)'])
Exog['logGDPPCP'] = np.log(Exog['GDPPCP'])

#Create regression for estimating agricultural water demand
y = dat2['WaterWithdAgriculture']
y = y[y < 100]
X = Exog['LANDIRAREAEQUIPACTUAL']
dropNulls = pd.concat([y, X], axis=1)
dropNulls.dropna(inplace=True)
y = dropNulls['WaterWithdAgriculture']
X = dropNulls['LANDIRAREAEQUIPACTUAL']
X = sm.add_constant(X)
model = sm.OLS(y, X, data=dropNulls, missing='drop')
AgModel = model.fit()
#print(AgModel.summary())
LandEquip = Exog['LANDIRAREAEQUIPACTUAL']
LandEquip.dropna(inplace=True)
LandEquip = sm.add_constant(LandEquip)
LandEquip['predict'] = AgModel.predict(LandEquip)
LandEquip[LandEquip['predict'] < 0] = 0.005

#Create regression for estimating municipal water demand
WaterWithdMunicipal = dat2['WaterWithdMunicipal']
POPURBAN = Exog['POPURBAN']
y = pd.concat([WaterWithdMunicipal, POPURBAN], axis=1)
y.dropna(inplace=True)
y['MunicipalPerCapita'] = y['WaterWithdMunicipal'] / y['POPURBAN']
y = y['MunicipalPerCapita']
X = Exog[['logUrbanPercent', 'logWATSAFE(piped)', 'logGDPPCP']]
dropNulls = pd.concat([y, X], axis=1)
dropNulls.dropna(inplace=True)
y = dropNulls['MunicipalPerCapita']
X = dropNulls[['logUrbanPercent', 'logWATSAFE(piped)', 'logGDPPCP']]
X = sm.add_constant(X)
model = sm.OLS(y, X, data=dropNulls, missing='drop')
MunModel = model.fit()
#print(AgModel.summary())
MunIVs = Exog[['logUrbanPercent', 'logWATSAFE(piped)', 'logGDPPCP']]
MunIVs = sm.add_constant(MunIVs)
MunIVs['predict'] = MunModel.predict(MunIVs)
MunIVs['POPURBAN'] = Exog['POPURBAN']
MunIVs['predict'] = MunIVs['predict'] * MunIVs['POPURBAN']
MunIVs[MunIVs['predict'] < 0] = 0.005

#Create regression for estimating industrial water demand
y = dat2['WaterWithdIndustrial']
# y = y[y<100]
X = Exog['VADD(man)']
dropNulls = pd.concat([y, X], axis=1)
dropNulls.dropna(inplace=True)
y = dropNulls['WaterWithdIndustrial']
X = dropNulls['VADD(man)']
X = sm.add_constant(X)
model = sm.OLS(y, X, data=dropNulls, missing='drop')
IndModel = model.fit()
# print(IndModel.summary())
IndIV = Exog['VADD(man)']
IndIV.dropna(inplace=True)
IndIV = sm.add_constant(IndIV)
IndIV['predict'] = IndModel.predict(IndIV)
IndIV[IndIV['predict'] < 0] = 0.005

#Create regression for estimating total renewable water resources
y = dat2['WaterResTotalRenew']
X = Exog['LANDAREA']
dropNulls = pd.concat([y, X], axis=1)
dropNulls.dropna(inplace=True)
y = dropNulls['WaterResTotalRenew']
X = dropNulls['LANDAREA']
X = sm.add_constant(X)
model = sm.OLS(y, X, data=dropNulls, missing='drop')
TRWRModel = model.fit()
# print(IndModel.summary())
TRWRIV = Exog['LANDAREA']
TRWRIV.dropna(inplace=True)
TRWRIV = sm.add_constant(TRWRIV)
TRWRIV['predict'] = TRWRModel.predict(TRWRIV)
TRWRIV[TRWRIV['predict'] < 0] = 0.005

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
#Estimate ag water demand with area of land actually irrigated
    if np.isnan(dat2.loc[[country], ['WaterWithdAgriculture']].values[0]):
        if country in LandEquip.index:
            dat2.loc[[country], ['WaterWithdAgriculture']] = (LandEquip.loc[[country], ['predict']].values[0])
#Estimate municipal water demand per capita using UrbanPop(percent), GDPPCP, and WATSAFE and then multiply it by POPURBAN
    if np.isnan(dat2.loc[[country], ['WaterWithdMunicipal']].values[0]):
        #print(IVs.loc[['Serbia'],:]
        if country in MunIVs.index:
            dat2.loc[[country], ['WaterWithdMunicipal']] = (MunIVs.loc[[country], ['predict']].values[0])
#Estimate industrial water demand using value added from the manufacturing sector
    if np.isnan(dat2.loc[[country], ['WaterWithdIndustrial']].values[0]):
        if country in IndIV.index:
            dat2.loc[[country], ['WaterWithdIndustrial']] = (IndIV.loc[[country], ['predict']].values[0])
#Normalize water demand so that they sum to total
    if not np.isnan(dat2.loc[[country], ['WaterTotalWithd']].values[0]):
        EmpiricalTotal = dat2.loc[[country], ['WaterTotalWithd']].values[0]
        Municipal = dat2.loc[[country], ['WaterWithdMunicipal']].values[0]
        Industrial = dat2.loc[[country], ['WaterWithdIndustrial']].values[0]
        Agriculture = dat2.loc[[country], ['WaterWithdAgriculture']].values[0]
        WaterDemandTotal = Municipal + Industrial + Agriculture
        dat2.loc[[country], ['WaterWithdMunicipal']] = (Municipal / WaterDemandTotal) * EmpiricalTotal
        dat2.loc[[country], ['WaterWithdIndustrial']] = (Industrial / WaterDemandTotal) * EmpiricalTotal
        dat2.loc[[country], ['WaterWithdAgriculture']] = (Agriculture / WaterDemandTotal) * EmpiricalTotal

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
                (dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0] + dat2.loc[[country], ['WaterGroundTotal']].values[0])

#Now, either we have no data for total, surface, or ground, or we only have data for one of them.
#If we have no data for any then we estimate total renewable water resources (TRWR) using land area
#Then assume 71-29 split between surface and ground
    if np.isnan(dat2.loc[[country], ['WaterResTotalRenew']].values[0]) and \
        np.isnan(dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0]) and \
                 np.isnan(dat2.loc[[country], ['WaterGroundTotal']].values[0]):
        dat2.loc[[country], ['WaterResTotalRenew']] = (TRWRIV.loc[[country], ['predict']].values[0])
        dat2.loc[[country], ['WaterResTotalRenewSurface']] = dat2.loc[[country], ['WaterResTotalRenew']].values[0] * 0.71
        dat2.loc[[country], ['WaterGroundTotal']] = dat2.loc[[country], ['WaterResTotalRenew']].values[0] * 0.29
#If we only have data for total:
    if not np.isnan(dat2.loc[[country], ['WaterResTotalRenew']].values[0]) and \
        np.isnan(dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0]) and \
                 np.isnan(dat2.loc[[country], ['WaterGroundTotal']].values[0]):
        dat2.loc[[country], ['WaterResTotalRenewSurface']] = dat2.loc[[country], ['WaterResTotalRenew']].values[0] * 0.71
        dat2.loc[[country], ['WaterGroundTotal']] = dat2.loc[[country], ['WaterResTotalRenew']].values[0] * 0.29
#If we only have data for surface:
    if not np.isnan(dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0]) and \
        np.isnan(dat2.loc[[country], ['WaterResTotalRenew']].values[0]) and \
                 np.isnan(dat2.loc[[country], ['WaterGroundTotal']].values[0]):
        dat2.loc[[country], ['WaterResTotalRenew']] = dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0] / 0.71
        dat2.loc[[country], ['WaterGroundTotal']] = dat2.loc[[country], ['WaterResTotalRenew']].values[0] * 0.29
#If we only have data for ground:
    if not np.isnan(dat2.loc[[country], ['WaterGroundTotal']].values[0]) and \
        np.isnan(dat2.loc[[country], ['WaterResTotalRenew']].values[0]) and \
                 np.isnan(dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0]):
        dat2.loc[[country], ['WaterResTotalRenew']] = dat2.loc[[country], ['WaterGroundTotal']].values[0] / 0.29
        dat2.loc[[country], ['WaterResTotalRenewSurface']] = dat2.loc[[country], ['WaterResTotalRenew']].values[0] * 0.71
#Subtract overlap of surface and ground and reestimate surface and ground:
    if not np.isnan(dat2.loc[[country], ['WaterResOverlap']].values[0]):
        Overlap = dat2.loc[[country], ['WaterResOverlap']].values[0]
        SurfacePlusGround = dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0] + \
                            dat2.loc[[country], ['WaterGroundTotal']].values[0]
        dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0] = \
            (dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0] / SurfacePlusGround) * (SurfacePlusGround - Overlap)
        dat2.loc[[country], ['WaterGroundTotal']].values[0] = \
            (dat2.loc[[country], ['WaterGroundTotal']].values[0] / SurfacePlusGround) * (SurfacePlusGround - Overlap)

    dat2.loc[[country], ['WaterResTotalRenew']].values[0] = (dat2.loc[[country], ['WaterGroundTotal']].values[0]) + \
                                                        (dat2.loc[[country], ['WaterResTotalRenewSurface']].values[0])

##Exploitable water resources
#If we have total and surface, estimate ground
#If we have total and ground, estimate surface
#If we have surface and ground, estimate total


#Write to excel
writer= pd.ExcelWriter('AQUASTATForModel3.xlsx',engine='xlsxwriter')
#dataForModel.to_excel(writer,sheet_name='1',merge_cells=False)
dat2.to_excel(writer,sheet_name='1',merge_cells=False)
writer.save()

