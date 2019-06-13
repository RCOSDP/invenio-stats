# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Utilities for Invenio-Stats."""

from __future__ import absolute_import, print_function

import calendar
import os
from base64 import b64encode

import six
from dateutil import parser
from elasticsearch_dsl import Search
from flask import current_app, request, session
from flask_login import current_user
from geolite2 import geolite2
from invenio_cache import current_cache
from invenio_search import current_search_client
from werkzeug.utils import import_string

from . import config
from .proxies import current_stats


def get_anonymization_salt(ts):
    """Get the anonymization salt based on the event timestamp's day."""
    salt_key = 'stats:salt:{}'.format(ts.date().isoformat())
    salt = current_cache.get(salt_key)
    if not salt:
        salt_bytes = os.urandom(32)
        salt = b64encode(salt_bytes).decode('utf-8')
        current_cache.set(salt_key, salt, timeout=60 * 60 * 24)
    return salt


def get_geoip(ip):
    """Lookup country for IP address."""
    reader = geolite2.reader()
    ip_data = reader.get(ip) or {}
    return ip_data.get('country', {}).get('iso_code')


def get_user():
    """User information.

    .. note::

       **Privacy note** A users IP address, user agent string, and user id
       (if logged in) is sent to a message queue, where it is stored for about
       5 minutes. The information is used to:

       - Detect robot visits from the user agent string.
       - Generate an anonymized visitor id (using a random salt per day).
       - Detect the users host contry based on the IP address.

       The information is then discarded.
    """
    return dict(
        ip_address=request.remote_addr,
        user_agent=request.user_agent.string,
        user_id=(
            current_user.get_id() if current_user.is_authenticated else None
        ),
        session_id=session.get('sid_s')
    )


def obj_or_import_string(value, default=None):
    """Import string or return object.

    :params value: Import path or class object to instantiate.
    :params default: Default object to return if the import fails.
    :returns: The imported object.
    """
    if isinstance(value, six.string_types):
        return import_string(value)
    elif value:
        return value
    return default


def load_or_import_from_config(key, app=None, default=None):
    """Load or import value from config.

    :returns: The loaded value.
    """
    app = app or current_app
    imp = app.config.get(key)
    return obj_or_import_string(imp, default=default)


AllowAllPermission = type('Allow', (), {
    'can': lambda self: True,
    'allows': lambda *args: True,
})()


def default_permission_factory(query_name, params):
    """Default permission factory.

    It enables by default the statistics if they don't have a dedicated
    permission factory.
    """
    from invenio_stats import current_stats
    if current_stats.queries[query_name].permission_factory is None:
        return AllowAllPermission
    else:
        return current_stats.queries[query_name].permission_factory(
            query_name, params
        )


def get_aggregations(index, aggs_query):
    """Get aggregations.

    :param index:
    :param aggs_query:
    :return:
    """
    results = {}
    if index and aggs_query and 'aggs' in aggs_query:
        from invenio_indexer.api import RecordIndexer
        results = RecordIndexer().client.search(
            index=index, body=aggs_query)['aggregations']

    return results


class QueryFileReportsHelper(object):
    """Helper for parsing elasticsearch aggregations."""

    @classmethod
    def calc_file_stats_reports(cls, res, data_list):
        """Create response object for file_stats_reports."""
        for file in res['buckets']:
            for index in file['buckets']:
                data = {}
                data['file_key'] = file['key']
                data['index_list'] = index['key']
                data['total'] = index['value']
                data['admin'] = 0
                data['reg'] = 0
                data['login'] = 0
                data['no_login'] = 0
                data['site_license'] = 0
                for user in index['buckets']:
                    for license in user['buckets']:
                        if license['key'] == 1:
                            data['site_license'] += license['value']
                            break
                    userrole = user['key']
                    count = user['value']
                    if userrole == 'guest':
                        data['no_login'] += count
                    elif userrole == 'Contributor':
                        data['reg'] += count
                        data['login'] += count
                    elif 'Administrator' in userrole:
                        data['admin'] += count
                        data['login'] += count
                    else:
                        data['login'] += count
                data_list.append(data)

    @classmethod
    def calc_file_per_using_report(cls, res, data_list):
        """Create response object for file_per_using_report."""
        # file-download
        for item in res['get-file-download-per-user-report']['buckets']:
            data = {}
            data['cur_user_id'] = item['key']
            data['total_download'] = item['value']
            data_list.update({item['key']: data})
        # file-preview
        for item in res['get-file-preview-per-user-report']['buckets']:
            data = {}
            data['cur_user_id'] = item['key']
            data['total_preview'] = item['value']
            if data_list.get(item['key']):
                data_list[item['key']].update(data)
            else:
                data_list.update({item['key']: data})

    @classmethod
    def Calculation(cls, res, data_list):
        """Calculation."""
        if res['buckets'] is not None:
            cls.calc_file_stats_reports(res, data_list)
        elif res['get-file-download-per-user-report'] is not None \
                and res['get-file-preview-per-user-report'] is not None:
            cls.calc_file_per_using_report(res, data_list)

    @classmethod
    def get_file_stats_report(cls, **kwargs):
        """Get file download/preview report."""
        result = {}
        all_list = []
        open_access_list = []

        event = kwargs.get('event')
        year = kwargs.get('year')
        month = kwargs.get('month')

        try:
            query_month = str(year) + '-' + str(month).zfill(2)
            _, lastday = calendar.monthrange(year, month)
            all_params = {'start_date': query_month + '-01',
                          'end_date':
                          query_month + '-' + str(lastday).zfill(2)
                          + 'T23:59:59'}
            params = {'start_date': query_month + '-01',
                      'end_date':
                      query_month + '-' + str(lastday).zfill(2)
                      + 'T23:59:59',
                      'accessrole': 'open_access'}

            all_query_name = ''
            open_access_query_name = ''
            if event == 'file_download':
                all_query_name = 'get-file-download-report'
                open_access_query_name = 'get-file-download-open-access-report'
            elif event == 'file_preview':
                all_query_name = 'get-file-preview-report'
                open_access_query_name = 'get-file-preview-open-access-report'

            # all
            all_query_cfg = current_stats.queries[all_query_name]
            all_query = all_query_cfg.query_class(**all_query_cfg.query_config)
            all_res = all_query.run(**params)
            cls.Calculation(all_res, all_list)

            # open access
            open_access_query_cfg = current_stats.queries[open_access_query_name]
            open_access = open_access_query_cfg.query_class(
                **open_access_query_cfg.query_config)
            open_access_res = open_access.run(**params)
            cls.Calculation(open_access_res, open_access_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list
        result['open_access'] = open_access_list
        return result

    @classmethod
    def get_file_per_using_report(cls, **kwargs):
        """Get File Using Per User report."""
        result = {}
        all_list = {}
        all_res = {}

        year = kwargs.get('year')
        month = kwargs.get('month')

        try:
            query_month = str(year) + '-' + str(month).zfill(2)
            _, lastday = calendar.monthrange(year, month)
            params = {'start_date': query_month + '-01',
                      'end_date': query_month + '-' + str(lastday).zfill(2)
                      + 'T23:59:59'}

            all_query_name = ['get-file-download-per-user-report',
                              'get-file-preview-per-user-report']
            for query in all_query_name:
                all_query_cfg = current_stats.queries[query]
                all_query = all_query_cfg.\
                    query_class(**all_query_cfg.query_config)
                all_res[query] = all_query.run(**params)
            cls.Calculation(all_res, all_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list

        return result

    @classmethod
    def get(cls, **kwargs):
        """Get file reports."""
        event = kwargs.get('event')
        if event == 'file_download' or event == 'file_preview':
            return cls.get_file_stats_report(**kwargs)
        elif event == 'file_using_per_user':
            return cls.get_file_per_using_report(**kwargs)
        else:
            return []


class QuerySearchReportHelper(object):
    """Search Report helper."""

    @classmethod
    def parse_bucket_response(cls, raw_res, pretty_result):
        """Parsing bucket response."""
        if 'buckets' in raw_res:
            field_name = raw_res['field']
            value = raw_res['buckets'][0]['key']
            pretty_result[field_name] = value
            return cls.parse_bucket_response(
                raw_res['buckets'][0], pretty_result)
        else:
            return pretty_result

    @classmethod
    def get(cls, **kwargs):
        """Get number of searches per keyword."""
        result = {}
        year = kwargs.get('year')
        month = kwargs.get('month')

        try:
            query_month = str(year) + '-' + str(month).zfill(2)
            _, lastday = calendar.monthrange(year, month)
            start_date = query_month + '-01'
            end_date = query_month + '-' + str(lastday).zfill(2) + 'T23:59:59'
            result['date'] = query_month
            params = {'start_date': query_month + '-01',
                      'end_date': query_month + '-' + str(lastday).zfill(2)
                      + 'T23:59:59'}

            # Run query
            keyword_query_cfg = current_stats.queries['get-search-report']
            keyword_query = keyword_query_cfg.query_class(
                **keyword_query_cfg.query_config)
            raw_result = keyword_query.run(**params)

            all = []
            for report in raw_result['buckets']:
                current_report = {}
                current_report['search_key'] = report['key']
                pretty_report = cls.parse_bucket_response(
                    report, current_report)
                all.append(pretty_report)
            result['all'] = all

        except Exception as e:
            current_app.logger.debug(e)
            return {}

        return result


class QueryCommonReportsHelper(object):
    """CommonReports helper class."""

    @classmethod
    def get_common_params(cls, year, month):
        """Get common params."""
        query_month = str(year) + '-' + str(month).zfill(2)
        _, lastday = calendar.monthrange(year, month)
        params = {'start_date': query_month + '-01',
                  'end_date': query_month + '-' + str(lastday).zfill(2)
                  + 'T23:59:59'}
        return query_month, params

    @classmethod
    def get(cls, **kwargs):
        """Get file reports."""
        event = kwargs.get('event')
        if event == 'top_page_access':
            return cls.get_top_page_access_report(**kwargs)
        elif event == 'site_access':
            return cls.get_site_access_report(**kwargs)
        else:
            return []

    @classmethod
    def get_top_page_access_report(cls, **kwargs):
        """Get toppage access report."""
        def Calculation(res, data_list):
            """Calculation."""
            for item in res['top-view-total']['buckets']:
                for hostaccess in item['buckets']:
                    data = {}
                    data['host'] = hostaccess['key']
                    data['ip'] = item['key']
                    data['count'] = hostaccess['value']
                    data_list.update({item['key']: data})

        result = {}
        all_list = {}
        all_res = {}

        year = kwargs.get('year')
        month = kwargs.get('month')

        try:
            query_month, params = cls.get_common_params(year, month)
            all_query_name = ['top-view-total']
            for query in all_query_name:
                all_query_cfg = current_stats.queries[query]
                all_query = all_query_cfg.\
                    query_class(**all_query_cfg.query_config)
                all_res[query] = all_query.run(**params)
            Calculation(all_res, all_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list

        return result

    @classmethod
    def get_site_access_report(cls, **kwargs):
        """Get site access report."""
        def Calculation(query_list, res, site_license_list, other_list,
                        institution_name_list):
            """Calculation."""
            mapper = {}
            for k in query_list:
                items = res.get(k)
                site_license_list[k] = 0
                other_list[k] = 0
                if items:
                    for i in items['buckets']:
                        if i['key'] == '':
                            other_list[k] += i['value']
                        else:
                            site_license_list[k] += i['value']
                            if i['key'] in mapper:
                                institution_name_list[mapper[i['key']]
                                                      ][k] = i['value']
                            else:
                                mapper[i['key']] = len(institution_name_list)
                                data = {}
                                data['name'] = i['key']
                                data[k] = i['value']
                                institution_name_list.append(data)
            for k in query_list:
                for i in range(len(institution_name_list)):
                    if k not in institution_name_list[i]:
                        institution_name_list[i][k] = 0

        result = {}
        all_res = {}
        site_license_list = {}
        other_list = {}
        institution_name_list = []

        year = kwargs.get('year')
        month = kwargs.get('month')

        query_list = ['top_view', 'search', 'record_view',
                      'file_download', 'file_preview']

        try:
            query_month, params = cls.get_common_params(year, month)
            for q in query_list:
                query_cfg = current_stats.queries['get-' + q.replace('_', '-')
                                                  + '-per-site-license']
                query = query_cfg.query_class(**query_cfg.query_config)
                all_res[q] = query.run(**params)
            Calculation(query_list, all_res, site_license_list, other_list,
                        institution_name_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['site_license'] = [site_license_list]
        result['other'] = [other_list]
        result['institution_name'] = institution_name_list
        return result


class QueryRecordViewPerIndexReportHelper(object):
    """RecordViewPerIndex helper class."""

    nested_path = 'record_index_list'
    first_level_field = 'record_index_list.index_id'
    second_level_field = 'record_index_list.index_name'

    @classmethod
    def get_nested_agg(cls, start_date, end_date):
        """Get nested aggregation by index id."""
        agg_query = Search(using=current_search_client,
                           index='events-stats-record-view',
                           doc_type='stats-record-view')[0:0]

        if start_date is not None and end_date is not None:
            time_range = {}
            time_range['gte'] = parser.parse(start_date).isoformat()
            time_range['lte'] = parser.parse(end_date).isoformat()
            agg_query = agg_query.filter(
                'range', **{'timestamp': time_range}).filter(
                'term', **{'is_restricted': False})
        agg_query.aggs.bucket(cls.nested_path, 'nested',
                              path=cls.nested_path) \
            .bucket(cls.first_level_field, 'terms',
                    field=cls.first_level_field, size=0) \
            .bucket(cls.second_level_field, 'terms',
                    field=cls.second_level_field, size=0)
        return agg_query.execute().to_dict()

    @classmethod
    def parse_bucket_response(cls, res, date):
        """Parse raw aggregation response."""
        aggs = res['aggregations'][cls.nested_path]
        result = {'date': date, 'all': [], 'total': aggs['doc_count']}
        for id_agg in aggs[cls.first_level_field]['buckets']:
            for name_agg in id_agg[cls.second_level_field]['buckets']:
                result['all'].append({'index_name': name_agg['key'],
                                      'view_count': id_agg['doc_count']})
        return result

    @classmethod
    def get(cls, **kwargs):
        """Get record view per index report.

        Nested aggregations are currently unsupported so manually aggregating.
        """
        result = {}
        year = kwargs.get('year')
        month = kwargs.get('month')

        try:
            query_month = str(year) + '-' + str(month).zfill(2)
            _, lastday = calendar.monthrange(year, month)
            start_date = query_month + '-01'
            end_date = query_month + '-' + str(lastday).zfill(2) + 'T23:59:59'
            raw_result = cls.get_nested_agg(start_date, end_date)
            result = cls.parse_bucket_response(raw_result, query_month)

        except Exception as e:
            current_app.logger.debug(e)
            return {}

        return result


class QueryRecordViewReportHelper(object):
    """RecordViewReport helper class."""

    @classmethod
    def Calculation(cls, res, data_list):
        """Create response object."""
        for item in res['buckets']:
            for record in item['buckets']:
                data = {}
                data['record_id'] = item['key']
                data['index_names'] = record['key']
                data['total_all'] = record['value']
                data['total_not_login'] = 0
                for user in record['buckets']:
                    if user['key'] == 'guest':
                        data['total_not_login'] += user['value']
                data_list.append(data)

    @classmethod
    def get(cls, **kwargs):
        """Get record view report."""
        result = {}
        all_list = []

        year = kwargs.get('year')
        month = kwargs.get('month')

        try:
            query_month = str(year) + '-' + str(month).zfill(2)
            _, lastday = calendar.monthrange(year, month)
            params = {'start_date': query_month + '-01',
                      'end_date':
                      query_month + '-' + str(lastday).zfill(2)
                      + 'T23:59:59'}

            all_query_cfg = current_stats.queries['get-record-view-report']
            all_query = all_query_cfg.query_class(**all_query_cfg.query_config)
            all_res = all_query.run(**params)
            cls.Calculation(all_res, all_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list

        return result
