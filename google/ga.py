#!/usr/bin/env python
# -*- coding: utf-8 -*-

from gdata.analytics.client import AnalyticsClient, DataFeedQuery

# Nome do nosso aplicativo.
APP_NAME = 'GA Feed'

class AnalyticsMetrics(object):
    '''
    Classe para obter informações do Google Analytics.
    '''

    def __init__(self, login, pwd):
        '''
        Instancia a classe AnalyticsMetrics.

        login: Login do Analytics.
        pwd: Senha do Analytics.
        '''
        # Criamos um cliente de conexão para o Analytics.
        self.client = AnalyticsClient(source=APP_NAME)
        # Efetua o login na API com usuário e senha.
        self.client.client_login(login, pwd, source=APP_NAME)

    def get_metrics(self, ids, start_date, end_date):
        '''
        Retorna métricas de um período para os IDs informados.

        ids: IDs dos sites. Ex: ga:12345678 ou ga:12345678,ga:87654321
        start_date: Data de início, no formato YYYY-MM-DD.
        end_date: Data fim, no formato YYYY-MM-DD.
        '''

        query_metrics = ['ga:users', # Quantide de usuários que acessou no período.
                         'ga:sessions', # Quantide de sessões no período.
                         'ga:pageviews', # Quantide de páginas visualizadas no período.
                         'ga:uniquePageviews', # Quantide de páginas visualizadas (únicas) no período.
                         'ga:avgSessionDuration', # Média de duração das sessões no período.
                         'ga:avgTimeOnPage', # Média de tempo por página no período.
                         'ga:percentNewSessions'] # Porcentagem de novos usuários no período.

        # DataFeedQuery é o nosso "SELECT" na API do Analytics.
        query = DataFeedQuery({
            'ids': ids,
            'start-date': start_date,
            'end-date': end_date,
            'metrics': ','.join(query_metrics)})

        # Executa a consulta.
        feed = self.client.GetDataFeed(query)

        metrics = {}

        # Para cada métrica consultada, eu obtenho o valor e jogo em um dicionário.
        for metric in query_metrics:
            met = feed.aggregates.get_metric(metric)

            if met:
                metrics[met.name] = met.value

        return metrics

    def get_pageviews(self, ids, start_date, end_date):
        '''
        Retorna a quantidade de visualizações de um site em um determinado período.

        ids: IDs dos sites. Ex: ga:12345678 ou ga:12345678,ga:87654321
        start_date: Data de início, no formato YYYY-MM-DD.
        end_date: Data fim, no formato YYYY-MM-DD.
        '''

        # DataFeedQuery é o nosso "SELECT" na API do Analytics.
        query = DataFeedQuery({
            'ids': ids,
            'start-date': start_date,
            'end-date': end_date,
            'metrics': 'ga:pageviews'})

        # Executa a consulta.
        feed = self.client.GetDataFeed(query)
        # Obtemos a quantidade de visualizações.
        met = feed.aggregates.get_metric('ga:pageviews')

        if met:
            return int(met.value)

        return 0

    def get_top_pages_count(self, ids, start_date, end_date, top_count=5):
        '''
        Retorna N páginas mais acessadas em um determinado período.

        ids: IDs dos sites. Ex: ga:12345678 ou ga:12345678,ga:87654321
        start_date: Data de início, no formato YYYY-MM-DD.
        end_date: Data fim, no formato YYYY-MM-DD.
        top_count: Quantidade de páginas a retornar (default: 5).
        '''

        # DataFeedQuery é o nosso "SELECT" na API do Analytics.
        # Diferente dos demais, neste possuímos ordenação DESC
        # pelo total de views de uma página.
        # Limitamos o resultado do SELECT a N registros.
        # Criamos uma dimensão, onde agrupamos as pageviews
        # por pagePath e pageTitle.
        query = DataFeedQuery({
            'ids': ids,
            'start-date': start_date,
            'end-date': end_date,
            'dimensions': 'ga:pagePath,ga:pageTitle',
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews',
            'max-results': top_count })

        # Executa a consulta.
        feed = self.client.GetDataFeed(query)

        metrics = []

        # Para cada dimensão, que no nosso caso é uma página do site,
        # pegamos as informações de URL, título e total de views.
        for entry in feed.entry:
            metrics.append({
                'ga:pagePath': entry.get_dimension('ga:pagePath').value,
                'ga:pageTitle': entry.get_dimension('ga:pageTitle').value,
                'ga:pageviews': entry.get_metric('ga:pageviews').value })

        return metrics
