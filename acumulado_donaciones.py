import string

import numpy as np
import pandas as pd

donacion = pd.read_csv('data-out/MiCochinitoData.csv')
#donacion = donacion.groupby([u'id', u'cve', u't', 'm', 'id2',
#                             'DesGeo'])[u'valor'].sum().reset_index(
#                                 name="valor")
file_acumulado = []
acumulado = []
for y in [2015, 2016, 2017]:
    by_year = donacion.loc[donacion["t"] == y]
    for t in [1, 2, 3, 4]:
        if y == 2017 and t == 4:
            break
        by_quarter = by_year.loc[by_year["m"] == t]
        actual = by_quarter[[u'id', u'cve', 'id2', 'valor', 'DesGeo']]

        acumulado.append(actual)

        aux = pd.concat(acumulado).groupby(
            [u'id', u'cve', 'id2', 'DesGeo'])['valor'].sum().reset_index(
                name="valor")
        aux["t"] = y
        aux["m"] = t

        file_acumulado.append(aux)

pd.concat(file_acumulado).to_csv('AcumuladoCochino.csv', index=False)
