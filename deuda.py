#!/usr/bin/env python
# -*- coding: utf-8 -*-
import io
import string

import numpy as np
import pandas as pd
import requests

#url = "http://www.correosdemexico.gob.mx/datosabiertos/cp/cpdescarga.txt"
url = 'cpdescarga.txt'
CP = pd.read_csv(url, sep='|', skiprows=1)

CP = CP[[u'd_codigo', u'c_estado']].drop_duplicates().rename(
    columns={"d_codigo": "codigo_postal"})

Prestadero3 = pd.read_csv('data/Deuda/kubo_usuario_ord.csv')
Prestadero3[u'codigo_postal'] = pd.to_numeric(
    Prestadero3[u'codigo_postal'], errors='coerce').fillna(0).astype(np.int64)
Prestadero3 = pd.merge(Prestadero3, CP, how='left', on=[u'codigo_postal'])

Prestadero1 = pd.read_csv('data/Deuda/proyecto.csv')
Prestadero2 = pd.read_csv('data/Deuda/fondeo_kubo.csv')

Prestadero1[u'plazo_meses'] = Prestadero1[u'plazo_meses'] // 30

Prestadero = pd.merge(Prestadero1, Prestadero3, how='left', on=[u'id_usuario'])
Prestadero2 = Prestadero2.rename(
    columns={"id_usuario_fondeador": "id_usuario"})
PrestaderoFondeadore = pd.merge(
    Prestadero2, Prestadero1, how='left', on=[u'id_usuario'])
Prestadero = Prestadero.rename(columns={"c_estado": "cve"})

Prestadero[u'fecha_aprobado'] = pd.to_datetime(Prestadero[u'fecha_aprobado'])
Prestadero[u'anio'] = Prestadero[u'fecha_aprobado'].map(
    lambda x: x.year).fillna(0).astype(np.int64)
Prestadero[u'mes'] = Prestadero[u'fecha_aprobado'].map(
    lambda x: x.month).fillna(0).astype(np.int64)

T1 = Prestadero.groupby([u'anio', u'mes']).size().reset_index(
    name="ProyectosTotales")
T1['cve'] = 0

T1_a = Prestadero.groupby([u'anio', u'mes', u'categoria']).size().reset_index(
    name="ProyectosTotales")
T1_a['cve'] = 0

Prestadero.loc[Prestadero[u'indicador_para_genero'] == 'M',
               u'genero'] = "Masculino"

Prestadero.loc[Prestadero[u'indicador_para_genero'] == 'F',
               u'genero'] = "Femenino"

Prestadero[u'genero'] = Prestadero[u'genero'].fillna("")

T1_b = Prestadero.groupby([u'anio', u'mes', u'genero']).size().reset_index(
    name="ProyectosTotales")
T1_b['cve'] = 0
Prestadero[u'estado_civil'] = Prestadero[u'estado_civil'].fillna("")

T1_c = Prestadero.groupby([u'anio', u'mes',
                           u'estado_civil']).size().reset_index(
                               name="ProyectosTotales")
T1_c['cve'] = 0

T2 = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                    Prestadero[u'monto_solicitado']].groupby(
                        [u'anio', u'mes']).size().reset_index(
                            name="ProyectosTotales")

T2_a = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                      Prestadero[u'monto_solicitado']].groupby(
                          [u'anio', u'mes', u'categoria']).size().reset_index(
                              name="ProyectosTotales")

T2_b = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                      Prestadero[u'monto_solicitado']].groupby(
                          [u'anio', u'mes', u'genero']).size().reset_index(
                              name="ProyectosTotales")

T2_c = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                      Prestadero[u'monto_solicitado']].groupby([
                          u'anio', u'mes', u'estado_civil'
                      ]).size().reset_index(name="ProyectosTotales")

T3 = Prestadero.groupby([u'anio',
                         u'mes'])[u'monto_fondeado'].sum().reset_index(
                             name="ProyectosTotales")

T3_a = Prestadero.groupby([u'anio', u'mes',
                           'categoria'])[u'monto_fondeado'].sum().reset_index(
                               name="ProyectosTotales")

T3_b = Prestadero.groupby([u'anio', u'mes',
                           'genero'])[u'monto_fondeado'].sum().reset_index(
                               name="ProyectosTotales")
T3_c = Prestadero.groupby(
    [u'anio', u'mes', 'estado_civil'])[u'monto_fondeado'].sum().reset_index(
        name="ProyectosTotales")

T4 = Prestadero.groupby([u'anio',
                         u'mes'])[u'monto_fondeado'].mean().reset_index(
                             name="ProyectosTotales")

T4_a = Prestadero.groupby(
    [u'anio', u'mes', u'categoria'])[u'monto_fondeado'].mean().reset_index(
        name="ProyectosTotales")

T4_b = Prestadero.groupby([u'anio', u'mes',
                           u'genero'])[u'monto_fondeado'].mean().reset_index(
                               name="ProyectosTotales")

T4_c = Prestadero.groupby(
    [u'anio', u'mes', u'estado_civil'])[u'monto_fondeado'].mean().reset_index(
        name="ProyectosTotales")

Prestadero['exitoso'] = (
    Prestadero.monto_fondeado >=
    Prestadero.monto_solicitado).apply(lambda x: 1 if x else 0)

T4_e = Prestadero.groupby([u'anio', u'mes',
                           u'exitoso'])[u'monto_fondeado'].mean().reset_index(
                               name="ProyectosTotales")

T5 = Prestadero.groupby([u'anio', u'mes'])[u'exitoso'].mean().reset_index(
    name="ProyectosTotales")

T5_a = Prestadero.groupby([u'anio', u'mes',
                           u'categoria'])[u'exitoso'].mean().reset_index(
                               name="ProyectosTotales")

T5_b = Prestadero.groupby([u'anio', u'mes',
                           u'genero'])[u'exitoso'].mean().reset_index(
                               name="ProyectosTotales")

T5_c = Prestadero.groupby([u'anio', u'mes',
                           u'estado_civil'])[u'exitoso'].mean().reset_index(
                               name="ProyectosTotales")

T6 = Prestadero.groupby([u'anio',
                         u'mes'])[u'tasa_anual_fraccion'].mean().reset_index(
                             name="ProyectosTotales")

T6_a = Prestadero.groupby(
    [u'anio', u'mes',
     u'categoria'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

T6_b = Prestadero.groupby(
    [u'anio', u'mes', u'genero'])[u'tasa_anual_fraccion'].mean().reset_index(
        name="ProyectosTotales")

T6_c = Prestadero.groupby(
    [u'anio', u'mes',
     u'estado_civil'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

T6_f = Prestadero.groupby(
    [u'anio', u'mes', u'exitoso'])[u'tasa_anual_fraccion'].mean().reset_index(
        name="ProyectosTotales")

T6_g = Prestadero.groupby(
    [u'anio', u'mes',
     u'plazo_meses'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

PrestaderoFondeadore[u'fecha_aprobado'] = pd.to_datetime(
    PrestaderoFondeadore[u'fecha'])
PrestaderoFondeadore[u'anio'] = PrestaderoFondeadore[u'fecha_aprobado'].map(
    lambda x: x.year).fillna(0).astype(np.int64)
PrestaderoFondeadore[u'mes'] = PrestaderoFondeadore[u'fecha_aprobado'].map(
    lambda x: x.month).fillna(0).astype(np.int64)

T7 = PrestaderoFondeadore.groupby(
    [u'anio', u'mes'])[u'id_usuario'].nunique().reset_index(
        name="ProyectosTotales")

Prestadero[u'fin'] = pd.to_datetime(
    Prestadero[u'fecha_liberado'], errors='coerce')
Prestadero[u'inicio'] = pd.to_datetime(Prestadero[u'fecha_aprobado'])
Prestadero['duracion'] = ((Prestadero.fin - Prestadero.inicio) /
                          np.timedelta64(1, 'D')).apply(np.ceil)

T8 = Prestadero.groupby([u'anio', u'mes'])[u'duracion'].mean().reset_index(
    name="ProyectosTotales")

T8_a = Prestadero.groupby([u'anio', u'mes',
                           u'categoria'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")

T8_b = Prestadero.groupby([u'anio', u'mes',
                           u'genero'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")

T8_c = Prestadero.groupby([u'anio', u'mes',
                           u'estado_civil'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")

T8_d = Prestadero.groupby([u'anio', u'mes',
                           u'exitoso'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")


def function(name):
    dataframe = eval(name)
    dataframe[u'cve'] = 0
    title = name.replace('_', '.')
    dataframe['Title'] = title
    if len(dataframe.columns) == 5:
        dataframe[u'Valor'] = ""
        dataframe[u'Clave'] = "Total"
        dataframe[u'id3'] = "Total"

    else:
        dataframe[u'Clave'] = list(dataframe)[2]
        dataframe = dataframe.rename(columns={dataframe.columns[2]: u'Valor'})
        dataframe[u'Valor'] = dataframe[u'Valor'].astype(str)
        print title
        dataframe[u'id3'] = dataframe[u'Clave'] + '-' + dataframe[u'Valor']

    return dataframe


Final = pd.concat([
    function(x)
    for x in [
        'T1', 'T1_a', 'T1_b', 'T1_c', 'T2', 'T2_a', 'T2_b', 'T2_c', 'T3',
        'T3_a', 'T3_b', 'T3_c', 'T4', 'T4_a', 'T4_b', 'T4_c', 'T4_e', 'T5',
        'T5_a', 'T5_b', 'T5_c', 'T6', 'T6_a', 'T6_b', 'T6_c', 'T6_f', 'T6_g',
        'T7', 'T8', 'T8_a', 'T8_b', 'T8_c', 'T8_d'
    ]
])

Prestadero[u'cve'] = Prestadero[u'cve'].fillna("")

T1 = Prestadero.groupby([u'cve', u'anio', u'mes']).size().reset_index(
    name="ProyectosTotales")

T1_a = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'categoria']).size().reset_index(
                               name="ProyectosTotales")

T1_b = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'genero']).size().reset_index(
                               name="ProyectosTotales")

T1_c = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'estado_civil']).size().reset_index(
                               name="ProyectosTotales")

T2 = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                    Prestadero[u'monto_solicitado']].groupby(
                        [u'cve', u'anio', u'mes']).size().reset_index(
                            name="ProyectosTotales")

T2_a = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                      Prestadero[u'monto_solicitado']].groupby([
                          u'cve', u'anio', u'mes', u'categoria'
                      ]).size().reset_index(name="ProyectosTotales")

T2_b = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                      Prestadero[u'monto_solicitado']].groupby([
                          u'cve', u'anio', u'mes', u'genero'
                      ]).size().reset_index(name="ProyectosTotales")

T2_c = Prestadero.loc[Prestadero[u'monto_fondeado'] >=
                      Prestadero[u'monto_solicitado']].groupby([
                          u'cve', u'anio', u'mes', u'estado_civil'
                      ]).size().reset_index(name="ProyectosTotales")

T3 = Prestadero.groupby([u'cve', u'anio',
                         u'mes'])[u'monto_fondeado'].sum().reset_index(
                             name="ProyectosTotales")

T3_a = Prestadero.groupby([u'cve', u'anio', u'mes',
                           'categoria'])[u'monto_fondeado'].sum().reset_index(
                               name="ProyectosTotales")

T3_b = Prestadero.groupby([u'cve', u'anio', u'mes',
                           'genero'])[u'monto_fondeado'].sum().reset_index(
                               name="ProyectosTotales")
T3_c = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     'estado_civil'])[u'monto_fondeado'].sum().reset_index(
         name="ProyectosTotales")

T4 = Prestadero.groupby([u'cve', u'anio',
                         u'mes'])[u'monto_fondeado'].mean().reset_index(
                             name="ProyectosTotales")

T4_a = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'categoria'])[u'monto_fondeado'].mean().reset_index(
         name="ProyectosTotales")

T4_b = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'genero'])[u'monto_fondeado'].mean().reset_index(
                               name="ProyectosTotales")

T4_c = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'estado_civil'])[u'monto_fondeado'].mean().reset_index(
         name="ProyectosTotales")

T4_e = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'exitoso'])[u'monto_fondeado'].mean().reset_index(
                               name="ProyectosTotales")

T5 = Prestadero.groupby([u'cve', u'anio',
                         u'mes'])[u'exitoso'].mean().reset_index(
                             name="ProyectosTotales")

T5_a = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'categoria'])[u'exitoso'].mean().reset_index(
                               name="ProyectosTotales")

T5_b = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'genero'])[u'exitoso'].mean().reset_index(
                               name="ProyectosTotales")

T5_c = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'estado_civil'])[u'exitoso'].mean().reset_index(
                               name="ProyectosTotales")

T6 = Prestadero.groupby([u'cve', u'anio',
                         u'mes'])[u'tasa_anual_fraccion'].mean().reset_index(
                             name="ProyectosTotales")

T6_a = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'categoria'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

T6_b = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'genero'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

T6_c = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'estado_civil'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

T6_f = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'exitoso'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")

T6_g = Prestadero.groupby(
    [u'cve', u'anio', u'mes',
     u'plazo_meses'])[u'tasa_anual_fraccion'].mean().reset_index(
         name="ProyectosTotales")
PrestaderoFondeadore['cve'] = ''
T7 = PrestaderoFondeadore.groupby(
    [u'cve', u'anio', u'mes'])[u'id_usuario'].nunique().reset_index(
        name="ProyectosTotales")

T8 = Prestadero.groupby([u'cve', u'anio',
                         u'mes'])[u'duracion'].mean().reset_index(
                             name="ProyectosTotales")

T8_a = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'categoria'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")

T8_b = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'genero'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")

T8_c = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'estado_civil'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")

T8_d = Prestadero.groupby([u'cve', u'anio', u'mes',
                           u'exitoso'])[u'duracion'].mean().reset_index(
                               name="ProyectosTotales")


def estatal(name):
    dataframe = eval(name)
    title = name.replace('_', '.')
    dataframe['Title'] = title
    if len(dataframe.columns) == 5:
        dataframe[u'Valor'] = ""
        dataframe[u'Clave'] = "Total"
        dataframe[u'id3'] = "Total"
    else:
        dataframe[u'Clave'] = list(dataframe)[3]
        dataframe = dataframe.rename(columns={dataframe.columns[3]: u'Valor'})
        dataframe[u'Valor'] = dataframe[u'Valor'].astype(str)
        dataframe[u'id3'] = dataframe[u'Clave'] + '-' + dataframe[u'Valor']

    return dataframe


Estatal = pd.concat([
    estatal(x)
    for x in [
        'T1', 'T1_a', 'T1_b', 'T1_c', 'T2', 'T2_a', 'T2_b', 'T2_c', 'T3',
        'T3_a', 'T3_b', 'T3_c', 'T4', 'T4_a', 'T4_b', 'T4_c', 'T4_e', 'T5',
        'T5_a', 'T5_b', 'T5_c', 'T6', 'T6_a', 'T6_b', 'T6_c', 'T6_f', 'T6_g',
        'T7', 'T8', 'T8_a', 'T8_b', 'T8_c', 'T8_d'
    ]
])

Prestadero = pd.concat([Estatal, Final])
s = Prestadero['Title'].apply(lambda x: x.split('.'))

Prestadero['Var1'] = s.apply(lambda x: x[0])
Prestadero['Var2'] = s.apply(lambda x: x[1] if len(x) == 2 else np.nan)
Prestadero = Prestadero.drop("Title", 1)

var1 = Prestadero['Var1'].unique()
ids = ['i1%s' % i for i in range(1, len(var1) + 1)]

IDs = pd.DataFrame({'Var1': var1, 'id': ids})
Prestadero = pd.merge(Prestadero, IDs, how='left', on=[u'Var1'])

ids2 = []
for id_name, dt in Prestadero[[
        u'id', u'id3'
]].dropna().drop_duplicates().dropna().groupby('id'):

    mapping_ids2 = dict(
        zip(dt[u'id3'].unique(), string.lowercase + string.ascii_uppercase))
    print id_name, dt, mapping_ids2
    dt['id2'] = dt['id3'].map(lambda x: mapping_ids2[x])
    ids2.append(dt)
ID2 = pd.concat(ids2)

Prestadero = pd.merge(Prestadero, ID2, how='left', on=[u'id3', u'id'])
Prestadero = Prestadero[[
    u'id', u'cve', u'anio', u'ProyectosTotales', u'mes', 'id2'
]]
Prestadero = Prestadero.rename(
    columns={u'anio': u't',
             u'ProyectosTotales': u'valor',
             u'mes': u'm'})
Prestadero[u'DesGeo'] = Prestadero[u'cve'].map(
    lambda x: 'N' if x == 0 else 'E')
Prestadero = Prestadero.drop_duplicates()

Prestadero = Prestadero.drop(
    Prestadero[Prestadero['cve'].map(lambda x: x == "")].index)
Prestadero = Prestadero.drop(
    Prestadero[Prestadero['t'].map(lambda x: x == 0)].index)

Prestadero[u'm'] = Prestadero[u'm'].map(
    lambda x: 1 if x < 4 else 2 if x < 7 else 3 if x < 9 else 4)

DesGeo = Prestadero[['id', 'DesGeo']].drop_duplicates()
RangeT = Prestadero[['id', 't', 'm']].drop_duplicates().rename(
    columns={"t": "ranget",
             "m": "rangem"})

Prestadero.to_csv('KuboData.csv', index=False)
DesGeo.to_csv('KuboDesGeo.csv', index=False)
RangeT.to_csv('KuboRangosTemporales.csv', index=False)
ID2.to_csv('KuboCodigosGrupos.csv', index=False)
