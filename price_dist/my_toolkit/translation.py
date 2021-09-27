#!/usr/bin/env python
# coding=utf-8
# author:marmot

import sys
import importlib

importlib.reload(sys)
# sys.setdefaultencoding('utf-8')

de_translation_dict = {"aktiviert": "enabled", "exakt": "exact", "weitgefasst": "broad",
                       "Manuell": "Manual", "negativ exakt": "negative exact", 'Kampagnen': 'Campaign',
                       'Anzeigengruppe': 'Ad Group', 'Anzeige': 'Ad',
                       'Produkt-Targeting': 'Product Targeting',
                       'Targeting-Ausdruck': 'Targeting Expression',
                       'targetingexpressionpredefined': 'Targeting Expression Predefined',
                       'targeting_expression_predefined': 'Targeting Expression Predefined',
                       }

es_translation_dict = {"habilitado": "enabled", "en pausa": "paused", "amplia": "broad",
                       "exacta": "exact", "frase": "phrase", "negativo frase": "negative phrase",
                       "negativo exacto": "negative exact", u'Campaña': 'Campaign',
                       'Nombre del grupo de anuncios': 'Ad Group', 'Anuncio': 'Ad',
                       'Palabra clave': 'Keyword',
                       'Direccionamiento por producto': 'Product Targeting',
                       u'Expresión de direccionamiento': 'Targeting Expression',
                       'targetingexpressionpredefined': 'Targeting Expression Predefined',
                       'targeting_expression_predefined': 'Targeting Expression Predefined',
                       }

it_translation_dict = {
    "abilitato": "enabled",
    "Manuale": "Manual",
    "in pausa": "paused",
    "generale": "broad",
    "esatta": "exact",
    "frase": "phrase",
    "frase negativa": "negative phrase",
    "esatto negativo": "negative exact",
    'campagna': 'Campaign',
    'Gruppo di annunci': 'Ad Group',
    'Annuncio pubblicitario': 'Ad',
    'Parola chiave': 'Keyword',
    'Targeting prodotto': 'Product Targeting',
    'Espressione del targeting': 'Targeting Expression',
    'targetingexpressionpredefined': 'Targeting Expression Predefined',
    'targeting_expression_predefined': 'Targeting Expression Predefined',
}

fr_translation_dict = {
    u'activé': "enabled",
    u"interrompu": "paused",
    "large": "broad",
    "expression": "phrase",
    "Manuel": "Manual",
    u"phrase négative": "negative phrase",
    u"négatif exact": "negative exact",
    'Campagne': 'Campaign',
    u'Groupe d’annonces': 'Ad Group',
    'pub': 'Ad', u'Mot-clé': 'Keyword',
    'Ciblage des produits': 'Product Targeting',
    "Cibler l'expression": "Targeting Expression",
    'targetingexpressionpredefined': 'Targeting Expression Predefined',
    'targeting_expression_predefined': 'Targeting Expression Predefined'
}

jp_translation_dict = {
    u"有効": "enabled",
    u"保留中": "paused",
    u"部分一致": "broad",
    u"完全一致": "exact",
    u"除外完全一致": "negative exact",
    u"除外フレーズ一致": "negative phrase",
    u"フレーズ一致": "phrase",
    u"オート": "Auto",
    u"マニュアル": "Manual",
    u'キャンペーン': 'Campaign',
    u'広告グループ': 'Ad Group',
    u'広告': 'Ad',
    u'キーワード': 'Keyword',
    u'商品ターゲティング': 'Product Targeting',
    u'ターゲティングの式': 'Targeting Expression',
    'targetingexpressionpredefined': 'Targeting Expression Predefined',
    'targeting_expression_predefined': 'Targeting Expression Predefined',
    u'事前に定義されたターゲティングの式': 'Targeting Expression Predefined'
}

mx_translation_dict = {'Palabra clave': 'Keyword',
                       'Grupo de anuncios': 'Ad Group',
                       u'Campaña': 'Campaign',
                       'Anuncio': 'Ad',
                       'exacta': 'exact',
                       'amplia': 'broad',
                       'en pausa': 'paused',
                       'targeting_expression_predefined': 'Targeting Expression Predefined',
                       'targetingexpressionpredefined': 'Targeting Expression Predefined'
                       }

all_language = {'US': '', 'CA': {}, 'AU': '',
                'DE': de_translation_dict, 'UK': '',
                'JP': jp_translation_dict, 'ES': es_translation_dict,
                'IT': it_translation_dict, 'FR': fr_translation_dict, 'MX': mx_translation_dict}


def station_judge(station):
    # 生成广告组名称
    if station == 'US':
        station_abbr = 'Amazon.com'
        language = {}
    elif station == 'CA':
        station_abbr = 'Amazon.ca'
        language = {}
    elif station == 'FR':
        station_abbr = 'Amazon.fr'
        language = fr_translation_dict
    elif station == 'UK':
        station_abbr = 'Amazon.co.uk'
        language = {}
    elif station == 'DE':
        station_abbr = 'Amazon.de'
        language = de_translation_dict
    elif station == 'ES':
        station_abbr = 'Amazon.es'
        language = es_translation_dict
    elif station == 'IT':
        station_abbr = 'Amazon.it'
        language = it_translation_dict
    elif station == 'JP':
        station_abbr = 'Amazon.jp'
        language = jp_translation_dict
    elif station == 'MX':
        station_abbr = 'Amazon.com.mx'
        language = mx_translation_dict
    elif station == 'IN':
        station_abbr = 'Amazon.in'
        language = {}

    station_lan = [station_abbr, language]
    return station_lan


def acos_choose(station):
    if station == 'US':
        acos = '20%'
    elif station == 'CA':
        acos = '15%'
    elif station == 'MX':
        acos = '10%'
    elif station == 'FR':
        acos = '10%'
    elif station == 'UK':
        acos = '15%'
    elif station == 'DE':
        acos = '10%'
    elif station == 'ES':
        acos = '10%'
    elif station == 'IT':
        acos = '10%'
    elif station == 'JP':
        acos = '10%'
    elif station == 'MX':
        acos = '10%'
    elif station == 'IN':
        acos = '10%'
    elif station == 'AU':
        acos = '10%'

    return acos


def cpcmin_choose(station):
    if station == 'JP':
        cpcmin = '2'
    elif station == 'MX':
        cpcmin = '0.1'
    elif station == 'IN':
        cpcmin = '1'
    elif station == 'AU':
        cpcmin = '0.1'
    else:
        cpcmin = '0.02'
    return cpcmin


def budget(station):
    if station == 'JP':
        budget_value = '100'
    else:
        budget_value = '2'
    return budget_value


def price_limit_listing(station):
    if station == 'JP':
        price = '500->1000000'
    elif station == 'MX':
        price = '100->1000000'
    elif station == 'IN':
        price = '400->1000000'
    elif station == 'AU':
        price = '7->100000'
    else:
        price = '7->10000'
    return price


def price_limit(station):
    if station == 'JP':
        price = '500'
    elif station == 'MX':
        price = '100'
    elif station == 'IN':
        price = '400'
    elif station == 'AU':
        price = '8'
    else:
        price = '7'
    return price


exchange_real_rate = {'JP': 100, 'MX': 20, 'IN': 72, 'US': 1, 'CA': 1, 'DE': 1,
                      'ES': 1, 'FR': 1, 'IT': 1, 'UK': 1, 'AU': 1.4}

exchange_rate = {'JP': [100, 100, 10000], 'MX': [5, 50, 3000], 'IN': [50, 100, 5000],
                 'US': [1, 2, 200], 'CA': [1, 2, 200], 'DE': [1, 2, 200],
                 'ES': [1, 2, 200], 'FR': [1, 2, 200], 'IT': [1, 2, 200],
                 'UK': [1, 2, 200], 'AU': [5, 2, 200]}

cpc_standard = {'CA': 0.16, 'DE': 0.11, 'ES': 0.09, 'FR': 0.11, 'IT': 0.08, 'JP': 7.72,
                'MX': 1.43, 'UK': 0.15, 'US': 0.21, 'IN': 1.19}

upper_acos = {'CA': 0.11, 'DE': 0.12, 'ES': 0.12, 'FR': 0.11, 'IT': 0.12, 'JP': 0.13,
              'MX': 0.11, 'UK': 0.14, 'US': 0.15, 'IN': 0.18}

net_stationabbr_dict = {'Amazon.com': 'US', 'Amazon.ca': 'CA', 'Amazon.mx': 'MX', 'Amazon.fr': 'FR',
                        'Amazon.co.uk': 'UK', 'Amazon.de': 'DE', 'Amazon.es': 'ES', 'Amazon.it': 'IT',
                        'Amazon.jp': 'JP', 'Amazon.com.mx': 'MX', 'Amazon.in': 'IN', 'Amazon.com.au': 'AU'}
