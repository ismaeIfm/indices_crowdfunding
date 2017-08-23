import string

import numpy as np
import pandas as pd

with open('data/Mi Cochinito.xlsx', 'r') as f:
    MC = pd.read_excel(f, sheetname='Proyecto')
with open('data/Mi Cochinito.xlsx', 'r') as f:
    MC2 = pd.read_excel(f, sheetname='Usuario')
with open('data/Mi Cochinito.xlsx', 'r') as f:
    MC3 = pd.read_excel(f, sheetname='Fondeo')

MC = MC.rename(columns={u'Usuario Creador (Anónimo)': u'Usuario'})
MCo = pd.merge(MC, MC2, how='left', on=[u'Usuario'])
MCo['pedido'] = MCo[u'Monto Pedido'].apply(
    lambda x: float(x.replace('$', '').replace(',', '')))
MCo['obtenido'] = MCo[u'Monto recaudado'].apply(
    lambda x: float(x.replace('$', '').replace(',', '')))
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
Re1_b = MCo.groupby([u'anio', u'mes', u'Sexo']).size().reset_index(
    name="ProyectosTotales")

Re2 = MCo.loc[MCo['exito'] == 1].groupby([u'anio', u'mes']).size().reset_index(
    name="ProyectosTotales")
Re2_a = MCo.loc[MCo['exito'] == 1].groupby([u'anio', u'mes',
                                            u'Categoría']).size().reset_index(
                                                name="ProyectosTotales")
Re2_b = MCo.loc[MCo['exito'] == 1].groupby([u'anio', u'mes',
                                            u'Sexo']).size().reset_index(
                                                name="ProyectosTotales")

Re3 = MCo.groupby([u'anio', u'mes'])['obtenido'].sum().reset_index(
    name="ProyectosTotales")
Re3_a = MCo.groupby([u'anio', u'mes',
                     u'Categoría'])['obtenido'].sum().reset_index(
                         name="ProyectosTotales")
Re3_b = MCo.groupby([u'anio', u'mes', u'Sexo'])['obtenido'].sum().reset_index(
    name="ProyectosTotales")
Re3_c = MCo.groupby([u'anio', u'mes', u'exito'])['obtenido'].sum().reset_index(
    name="ProyectosTotales")

Re4 = MCo.groupby([u'anio', u'mes'])['exito'].mean().reset_index(
    name="ProyectosTotales")
Re4_a = MCo.groupby([u'anio', u'mes',
                     u'Categoría'])['exito'].mean().reset_index(
                         name="ProyectosTotales")
Re4_b = MCo.groupby([u'anio', u'mes', u'Sexo'])['exito'].mean().reset_index(
    name="ProyectosTotales")

Re5 = MCFon.groupby(
    [u'anio', u'mes'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
        name="ProyectosTotales")
Re5_a = MCFon.groupby(
    [u'anio', u'mes',
     u'Sexo'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
         name="ProyectosTotales")

Re6 = MCFon.groupby(
    [u'anio', u'mes'])[u'Usuario Fondeador (Anónimo)'].nunique().reset_index(
        name="ProyectosTotales")

Re6_a = MCFon.groupby(
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
        dataframe[u'Clave'] = dataframe[u'Clave'] + '-' + dataframe[u'Valor']

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
