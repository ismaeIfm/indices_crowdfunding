#!/usr/bin/env python
# -*- coding: utf-8 -*-
import locale
import os
import time
from datetime import datetime

import pandas as pd
import requests

locale.setlocale(locale.LC_ALL, '')

TOKEN = os.environ.get('TOKEN')

categories_mapping = {'community': 'Comunitarios', 'innovation': 'Inventos'}


def get_micochinito(field):
    try:
        return requests.get('https://www.micochinito.com/api/%s?api_token=%s' %
                            (field, TOKEN)).json()
    except requests.exceptions.ConnectionError:
        time.sleep(1)
        return get_micochinito(field)


def get_micochinito_projects():
    MC = get_micochinito('projects')
    ids, users, categories, amount_goal, term, date_init, achieved = [], [], [], [], [], [], []
    for i in MC:
        ids.append(i['project_id'])
        users.append(i['owner_id'])
        categories.append(categories_mapping[i['project_category']])
        amount_goal.append(str(i['amount_goal']))
        term.append(i['project_duration'])
        date_init.append(
            datetime.strptime(i['project_starts_at']['date'],
                              "%Y-%m-%d %H:%M:%S.%f").strftime("%d/%m/%Y"))
        achieved.append(str(i['amout_achieved']))

    d = {
        u'ID Proyecto (Anónimo)': ids,
        u'Usuario Creador (Anónimo)': users,
        u'Categoría': categories,
        u'Monto Pedido': amount_goal,
        u'Plazo': term,
        u'Fecha inicio': date_init,
        u'Monto recaudado': achieved
    }
    return pd.DataFrame(
        data=d,
        columns=[
            u'ID Proyecto (Anónimo)', u'Usuario Creador (Anónimo)',
            u'Categoría', u'Subcategoría', u'Monto Pedido', u'Plazo',
            u'Fecha inicio', u'Fecha término', u'Monto recaudado'
        ])


def get_micochinito_donors():
    MC = get_micochinito('donors')
    users, gender, age = [], [], []
    for i in MC:
        users.append(i['user_id'])
        gender.append(i['user_gender'])
        age.append(i.get('user_age'))

    d = {u'Usuario': users, u'Sexo': gender, u'Edad': age}

    return pd.DataFrame(
        data=d,
        columns=[u'Usuario', u'Sexo', u'AñoNacimiento', u'Estado', u'Edad'])


def get_micochinito_donations():
    MC = get_micochinito('donations')
    users, amount, dates = [], [], []
    for i in MC:
        if i['status'] == 'paid':
            users.append(i['charge_id'])
            amount.append(i['amount_to_project'])
            dates.append(
                datetime.strptime(i['created_at']['date'],
                                  "%Y-%m-%d %H:%M:%S.%f").strftime("%d/%m/%Y"))
    d = {
        u'Usuario Fondeador (Anónimo)': users,
        u'Monto Fondeado': amount,
        u'Fecha': dates
    }
    return pd.DataFrame(
        data=d,
        columns=[
            u'ID Proyecto (Anónimo)', u'Usuario Fondeador (Anónimo)',
            u'Monto Fondeado', u'Fecha'
        ])


def create_excel(name):

    writer = pd.ExcelWriter(name)
    get_micochinito_projects().to_excel(writer, 'Proyecto', index=False)
    get_micochinito_donors().to_excel(writer, 'Usuario', index=False)
    get_micochinito_donations().to_excel(writer, 'Fondeo', index=False)
    writer.save()


if __name__ == '__main__':
    create_excel('data/Mi Cochinito 2017.xlsx')
