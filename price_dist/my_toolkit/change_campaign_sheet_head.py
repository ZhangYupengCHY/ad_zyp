#!/usr/bin/env python
# coding=utf-8
# author:marmot

import re


# from listing_to_auto_ad import normal_columns

def campaign_sheet_head(df):
    # 删除campaign中多余的列Product Targeting ID
    for uncalled_for in ['Product Targeting ID', u'製品ターゲットID', u'Producto dirigido a id', u'produkt für id',
                         'ID targeting prodotto', 'Produkt-Targeting-ID', 'ID de direccionamiento por producto',
                         'Identifiant de ciblage des produits', u'商品ターゲティングID', 'Portfolio ID', 'Placement Type',
                         'Increase bids by placement']:
        try:
            df.pop(uncalled_for)
        except:
            pass
    # 删除Unnamed：
    for one_clo in df.columns:
        if re.search(r'Unnamed:', one_clo):
            del df[one_clo]
    # 列名换成标志英文列名
    try:
        df.columns = normal_columns.campaign_columns1
    except ValueError:
        df.columns = normal_columns.campaign_columns2

    return df


de_translation = {'Datensatz-ID': 'Record ID',
                  'Datensatztyp': 'Record Type',
                  'Kampagnen': 'Campaign',
                  'Tagesbudget Kampagne': 'Campaign Daily Budget',
                  'Portfolio ID': 'Portfolio ID',
                  'Portfolio-ID': 'Portfolio ID',
                  'Startdatum der Kampagne': 'Campaign Start Date',
                  'Enddatum der Kampagne': 'Campaign End Date',
                  'Ausrichtungstyp der Kampagne': 'Campaign Targeting Type',
                  'Anzeigengruppe': 'Ad Group',
                  'Maximales Gebot': 'Max Bid', 'Schlagwort oder Product Targeting': 'Keyword',
                  'Produkt-Targeting-ID': 'Product Targeting ID',
                  u'Übereinstimmungstyp': 'Match Type', 'SKU': 'SKU',
                  'Kampagnenstatus': 'Campaign Status', 'Anzeigengruppe Status': 'Ad Group Status', 'Status': 'Status',
                  'Sichtkontakte': 'Impressions', 'Klicks': 'Clicks', 'Ausgaben': 'Spend', 'Bestellungen': 'Orders',
                  'Einheiten insgesamt': 'Total units', 'Vertrieb': 'Sales', 'ACoS': 'ACoS', 'Gebot+': 'Bid+'}

# FR站将'CPC max. '改为Enchère maximale-2019.07.29
fr_translation = {u'ID d’enregistrement': 'Record ID', u'Type d’enregistrement': 'Record Type',
                  u'Campagne ': 'Campaign',
                  u'Budget de campagne quotidien ': 'Campaign Daily Budget',
                  'Portfolio ID': 'Portfolio ID',
                  u'ID du portefeuille': 'Portfolio ID',
                  u'Date de début': 'Campaign Start Date', u'Date de fin ': 'Campaign End Date',
                  u'Type de ciblage ': 'Campaign Targeting Type', u'CPC max. ': 'Max Bid',
                  u'Groupe d’annonces': 'Ad Group', u'Enchère maximale': 'Max Bid',
                  u'Mot-clef ou ciblage de produit ': 'Keyword',
                  'Identifiant de ciblage des produits': 'Product Targeting ID',
                  'Type de correspondance': 'Match Type',
                  'SKU': 'SKU', 'Statut de la campagne': 'Campaign Status',
                  u'Statut du groupe de publicités': 'Ad Group Status', 'Statut': 'Status',
                  'Impressions': 'Impressions', 'Clics': 'Clicks', u'Dépense': 'Spend', 'Commandes': 'Orders',
                  u'Total des unités': 'Total units',
                  'Ventes': 'Sales', u'Coût publicitaire de vente (ACoS)': 'ACoS'}

es_translation = {'ID de registro': 'Record ID',
                  'Tipo de registro': 'Record Type',
                  u'Campaña': 'Campaign',
                  u'Presupuesto diario de campaña': 'Campaign Daily Budget',
                  u'ID del portafolio': 'Portfolio ID',
                  u'Fecha de inicio de la campaña': 'Campaign Start Date',
                  u'Fecha de finalización de la campaña': 'Campaign End Date',
                  u'Tipo de segmentación': 'Campaign Targeting Type',
                  'Nombre del grupo de anuncios': 'Ad Group',
                  u'Puja máxima': 'Max Bid',
                  'Palabra clave o direccionamiento del producto': 'Keyword',
                  'ID de direccionamiento por producto': 'Product Targeting ID',
                  'Tipo de coincidencia': 'Match Type',
                  'SKU': 'SKU',
                  u'Estado de campaña': 'Campaign Status',
                  'Estado de grupo de anuncios': 'Ad Group Status',
                  'Estado': 'Status',
                  'Impresiones': 'Impressions',
                  'Clics': 'Clicks',
                  'Gasto': 'Spend',
                  'Pedidos': 'Orders',
                  'Unidades totales': 'Total units',
                  'Ventas': 'Sales',
                  'ACoS': 'ACoS'}

it_translation = {'ID record': 'Record ID',
                  'Tipo di record': 'Record Type',
                  'campagna': 'Campaign',
                  'Budget giornaliero campagna': 'Campaign Daily Budget',
                  'Portfolio ID': 'Portfolio ID',
                  'ID portfolio': 'Portfolio ID',
                  'data di inizio della campagna': 'Campaign Start Date',
                  'data di fine della campagna': 'Campaign End Date',
                  'Tipo di targeting': 'Campaign Targeting Type',
                  'Gruppo di annunci': 'Ad Group',
                  'Offerta CPC': 'Max Bid',
                  'Parola chiave o targeting del prodotto': 'Keyword',
                  'ID targeting prodotto': 'Product Targeting ID',
                  'Tipo di corrispondenza': 'Match Type',
                  'Codice SKU': 'SKU',
                  'Stato campagna': 'Campaign Status',
                  'stato gruppo di annunci': 'Ad Group Status',
                  'Stato': 'Status',
                  'Impressioni': 'Impressions',
                  'Clic': 'Clicks',
                  'Spesa': 'Spend',
                  'Ordini': 'Orders',
                  u'Totale unità': 'Total units',
                  'Vendite': 'Sales',
                  'ACoS': 'ACoS'}

# 表头发生变化，需要变更，将'キーワードまたは製品ターゲティング'变为
# 'キーワードまたは商品ターゲティング'-2019.07.29
jp_translation = {u'レコードID': 'Record ID',
                  u'レコードタイプ': 'Record Type',
                  u'キャンペーン': 'Campaign',
                  u'キャンペーンの1日当たりの予算': 'Campaign Daily Budget',
                  'Portfolio ID': 'Portfolio ID',
                  u'キャンペーンの開始日': 'Campaign Start Date',
                  u'キャンペーンの終了日': 'Campaign End Date',
                  u'キャンペーンのターゲティングタイプ': 'Campaign Targeting Type',
                  u'広告グループ': 'Ad Group',
                  u'入札額': 'Max Bid',
                  u'キーワードまたは商品ターゲティング': 'Keyword', u'キーワードまたは製品ターゲティング': 'Keyword',
                  u'商品ターゲティングID': 'Product Targeting ID',
                  u'マッチタイプ': 'Match Type',
                  u'広告(SKU)': 'SKU',
                  u'キャンペーンステータス': 'Campaign Status',
                  u'広告グループステータス': 'Ad Group Status', u'ステータス': 'Status', u'インプレッション': 'Impressions',
                  u'クリック': 'Clicks', u'広告費': 'Spend',
                  u'注文数': 'Orders',
                  u'合計販売数': 'Total units', u'総売上': 'Sales',
                  u'売上高に占める広告費の割合 （ACoS)': 'ACoS',
                  # u'Bidding strategy':'Bidding strategy',
                  # u'広告枠の種類':'Placement Type',
                  # u'Increase bids by placement':'Increase bids by placement'
                  }

mx_translation = {'Identificador de registro': 'Record ID',
                  'Tipo de registro': 'Record Type',
                  u'Campaña': 'Campaign',
                  u'Presupuesto diario de campaña': 'Campaign Daily Budget',
                  u'Portfolio ID': 'Portfolio ID',
                  u'ID del pedido': 'Portfolio ID',
                  'Fecha de inicio': 'Campaign Start Date',
                  u'Fecha de finalización': 'Campaign End Date',
                  u'Tipo de segmentación': 'Campaign Targeting Type',
                  'Grupo de anuncios': 'Ad Group',
                  u'Puja máxima': 'Max Bid',
                  'Keyword or Product Targeting': 'Keyword',
                  u'Segmentación por producto o palabra clave': 'Keyword',
                  'Product Targeting ID': 'Product Targeting ID',
                  u'Identificador de segmentación por producto': 'Product Targeting ID',
                  'Tipo de coincidencia': 'Match Type',
                  'SKU': 'SKU',
                  u'Estado de la Campaña': 'Campaign Status',
                  'Estado del grupo de anuncios': 'Ad Group Status',
                  'Estado': 'Status',
                  'Impresiones': 'Impressions',
                  'Clics': 'Clicks',
                  u'Inversión': 'Spend',
                  'Pedidos': 'Orders',
                  'Total de unidades': 'Total units',
                  'Ventas': 'Sales',
                  'Costo publicitario de las ventas (ACoS)': 'ACoS'}

all_station = {'DE': de_translation, 'ES': es_translation, 'FR': fr_translation, 'IT': it_translation,
               'JP': jp_translation, 'MX': mx_translation}


def campaign_sheet_head_stand(df, station):
    if station not in ['US', 'UK', 'CA', 'IN']:
        trans_dict = all_station[station]
        # 删除campaign中多余的列Product Targeting ID
        for one_col in df.columns:
            if one_col in trans_dict.keys():
                df.rename(columns={one_col: trans_dict[one_col]}, inplace=True)
            else:
                try:
                    del df[one_col]
                except:
                    pass
    if 'Keyword or Product Targeting' in df.columns:
        df.rename(columns={'Keyword or Product Targeting': 'Keyword'}, inplace=True)
    if 'Total_Units' in df.columns:
        df.rename(columns={'Total_Units': 'Total_units'}, inplace=True)
    # 删除Unnamed：
    for one_clo in df.columns:
        if re.search(r'Unnamed:', one_clo):
            del df[one_clo]
    # 列名换成标志英文列名
    return df


if __name__ == "__main__":
    pass
