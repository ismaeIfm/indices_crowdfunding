#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import string

import numpy as np
import pandas as pd

fname = 'data/Mi Cochinito 2017.xlsx'
with open(fname, 'r') as f:
    MC = pd.read_excel(f, sheetname='Proyecto')
with open(fname, 'r') as f:
    MC2 = pd.read_excel(f, sheetname='Usuario')
with open(fname, 'r') as f:
    MC3 = pd.read_excel(f, sheetname='Fondeo')

MC = MC.rename(columns={u'Usuario Creador (Anónimo)': u'Usuario'})
MCo = pd.merge(MC, MC2, how='left', on=[u'Usuario'])

#MCo['pedido'] = MCo[u'Monto Pedido'].apply(
#    lambda x: float(x.replace('$', '').replace(',', '')))
MCo['pedido'] = MCo[u'Monto Pedido']

#MCo['obtenido'] = MCo[u'Monto recaudado'].apply(
#    lambda x: float(x.replace('$', '').replace(',', '')))
MCo['obtenido'] = MCo[u'Monto recaudado']

MCo['exito'] = (MCo.pedido <= MCo.obtenido).apply(lambda x: 1 if x else 0)

dates = MC3['Fecha'].str.split(',').apply(pd.Series, 1).stack()
dates.index = dates.index.droplevel(-1)
dates.name = 'Fecha'
MCFon = MC3.drop([u'ID Proyecto (Anónimo)', u'Fecha'], axis=1).join(dates)

MCFon2 = MCFon.groupby([u'Usuario Fondeador (Anónimo)']).size().reset_index(
    name="Pagos")

MCFon = pd.merge(
    MCFon, MCFon2, how='left', on=[u'Usuario Fondeador (Anónimo)'])
MCFon[u'Monto'] = pd.to_numeric(
    MCFon[u'Monto Fondeado'], errors='coerce') / MCFon[u'Pagos']

MCFon[u'fecha_aprobado'] = pd.to_datetime(MCFon[u'Fecha'], dayfirst=True)
MCFon[u'anio'] = MCFon[u'fecha_aprobado'].map(lambda x: x.year)
MCFon[u'mes'] = MCFon[u'fecha_aprobado'].map(lambda x: x.month)
MCFon[u'Usuario'] = MCFon[u'Usuario Fondeador (Anónimo)']

MCFon = pd.merge(MCFon, MC2, how='left', on=[u'Usuario'])

MCo[u'fecha_aprobado'] = pd.to_datetime(MCo[u'Fecha inicio'])
MCo[u'anio'] = MCo[u'fecha_aprobado'].map(lambda x: x.year)
MCo[u'mes'] = MCo[u'fecha_aprobado'].map(lambda x: x.month)

Re1 = MCo.groupby([u'anio', u'mes']).size().reset_index(
    name="ProyectosTotales")
Re1_a = MCo.groupby([u'anio', u'mes', u'Categoría']).size().reset_index(
    name="ProyectosTotales")
Re1_b = MCo.fillna("").groupby([u'anio', u'mes', u'Sexo']).size().reset_index(
    name="ProyectosTotales")

Re2 = MCo.loc[MCo['exito'] == 1].groupby([u'anio', u'mes']).size().reset_index(
    name="ProyectosTotales")
Re2_a = MCo.loc[MCo['exito'] == 1].groupby([u'anio', u'mes',
                                            u'Categoría']).size().reset_index(
                                                name="ProyectosTotales")
Re2_b = MCo.fillna("").loc[MCo['exito'] == 1].groupby(
    [u'anio', u'mes', u'Sexo']).size().reset_index(name="ProyectosTotales")

Re3 = MCo.groupby([u'anio', u'mes'])['obtenido'].sum().reset_index(
    name="ProyectosTotales")
Re3_a = MCo.groupby([u'anio', u'mes',
                     u'Categoría'])['obtenido'].sum().reset_index(
                         name="ProyectosTotales")
Re3_b = MCo.fillna("").groupby([u'anio', u'mes',
                                u'Sexo'])['obtenido'].sum().reset_index(
                                    name="ProyectosTotales")
Re3_c = MCo.groupby([u'anio', u'mes', u'exito'])['obtenido'].sum().reset_index(
    name="ProyectosTotales")

Re4 = MCo.groupby([u'anio', u'mes'])['exito'].mean().reset_index(
    name="ProyectosTotales")
Re4_a = MCo.groupby([u'anio', u'mes',
                     u'Categoría'])['exito'].mean().reset_index(
                         name="ProyectosTotales")
Re4_b = MCo.fillna("").groupby([u'anio', u'mes',
                                u'Sexo'])['exito'].mean().reset_index(
                                    name="ProyectosTotales")

Re5 = MCFon.fillna("").groupby(
    [u'anio', u'mes'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
        name="ProyectosTotales")
Re5_a = MCFon.fillna("").groupby(
    [u'anio', u'mes',
     u'Sexo'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
         name="ProyectosTotales")

Re6 = MCFon.fillna("").groupby(
    [u'anio', u'mes'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
        name="ProyectosTotales")
Re6_a = MCFon.fillna("").groupby(
    [u'anio', u'mes',
     u'Sexo'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
         name="ProyectosTotales")


def function(name):
    dataframe = eval(name)
    dataframe[u'cve'] = 0
    title = name.split('_')
    dataframe['Var1'] = title[0]
    if len(title) == 2:
        dataframe['Var2'] = title[1]
    if len(dataframe.columns) == 5:
        dataframe[u'Valor'] = ""
        dataframe[u'Clave'] = "Total"
        dataframe[u'id3'] = dataframe[u'Clave']
    else:
        dataframe[u'Clave'] = list(dataframe)[2]
        dataframe = dataframe.rename(columns={dataframe.columns[2]: u'Valor'})
        dataframe[u'Valor'] = dataframe[u'Valor'].astype(str)
        dataframe.loc[dataframe[u'Valor'] == '', u'Clave'] = "Total"
        dataframe.loc[dataframe[u'Valor'] == '', u'id3'] = dataframe[u'Clave']
        dataframe.loc[dataframe[u'Valor'] != '',
                      u'id3'] = dataframe[u'Clave'] + '-' + dataframe[u'Valor']

    return dataframe


MiCochinito = pd.concat([
    function(x)
    for x in [
        'Re1', 'Re1_a', 'Re1_b', 'Re2', 'Re2_a', 'Re2_b', 'Re3', 'Re3_a',
        'Re3_b', 'Re3_c', 'Re4', 'Re4_a', 'Re4_b', 'Re5', 'Re5_a', 'Re6',
        'Re6_a'
    ]
])
var1 = MiCochinito['Var1'].unique()
ids = ['i3%s' % i for i in range(1, len(var1) + 1)]

IDs = pd.DataFrame({'Var1': var1, 'id': ids})

MiCochinito = pd.merge(MiCochinito, IDs, how='left', on=[u'Var1'])

ids2 = []
for id_name, dt in MiCochinito[[u'id',
                                u'id3']].drop_duplicates().groupby('id'):
    mapping_ids2 = dict(zip(dt[u'id3'].unique(), string.lowercase))
    dt['id2'] = dt['id3'].map(lambda x: mapping_ids2[x])
    ids2.append(dt)
ID2 = pd.concat(ids2)

MiCochinito = pd.merge(MiCochinito, ID2, how='left', on=[u'id3', u'id'])

MiCochinito = MiCochinito[[
    u'id', u'cve', u'anio', u'ProyectosTotales', u'mes', 'id2'
]]
MiCochinito = MiCochinito.rename(
    columns={u'anio': u't',
             u'ProyectosTotales': u'valor',
             u'mes': u'm'})
MiCochinito[u'DesGeo'] = MiCochinito[u'cve'].map(
    lambda x: 'N' if x == 0 else 'E')
print MiCochinito[u't'].dtype
#MiCochinito = MiCochinito.loc[MiCochinito[u't'] != ""]
MiCochinito[u'm'] = MiCochinito[u'm'].map(
    lambda x: 1 if x < 4 else 2 if x < 7 else 3 if x < 9 else 4)

DesGeo = MiCochinito[['id', 'DesGeo']].drop_duplicates()
RangeT = MiCochinito[['id', 't', 'm']].drop_duplicates().rename(
    columns={"t": "ranget",
             "m": "rangem"})

MiCochinito.to_csv('MiCochinitoData.csv', index=False)
DesGeo.to_csv('MiCochinitoDesGeo.csv', index=False)
RangeT.to_csv('MiCochinitoRangosTemporales.csv', index=False)
ID2.to_csv('MiCochinitoCodigosGrupos.csv', index=False)
