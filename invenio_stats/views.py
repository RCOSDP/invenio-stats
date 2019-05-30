# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""InvenioStats views."""
import calendar
from datetime import datetime, timedelta
from math import ceil

import dateutil.relativedelta as relativedelta
from dateutil import parser
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Search
from flask import Blueprint, abort, current_app, jsonify, request
from invenio_rest.views import ContentNegotiatedMethodView
from invenio_search import current_search_client

from invenio_stats.utils import get_aggregations

from . import config
from .errors import InvalidRequestInputError, UnknownQueryError
from .proxies import current_stats
from .utils import current_user

blueprint = Blueprint(
    'invenio_stats',
    __name__,
    url_prefix='/stats',
)


class StatsQueryResource(ContentNegotiatedMethodView):
    """REST API resource providing access to statistics."""

    view_name = 'stat_query'

    def __init__(self, **kwargs):
        """Constructor."""
        super(StatsQueryResource, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def post(self, **kwargs):
        """Get statistics."""
        data = request.get_json(force=False)
        if data is None:
            data = {}
        result = {}
        for query_name, config in data.items():
            if config is None or not isinstance(config, dict) \
                    or (set(config.keys()) != {'stat', 'params'} and
                        set(config.keys()) != {'stat'}):
                raise InvalidRequestInputError(
                    'Invalid Input. It should be of the form '
                    '{ STATISTIC_NAME: { "stat": STAT_TYPE, '
                    r'"params": STAT_PARAMS \}}'
                )
            stat = config['stat']
            params = config.get('params', {})
            try:
                query_cfg = current_stats.queries[stat]
            except KeyError:
                raise UnknownQueryError(stat)

            permission = current_stats.permission_factory(stat, params)
            if permission is not None and not permission.can():
                message = ('You do not have a permission to query the '
                           'statistic "{}" with those '
                           'parameters'.format(stat))
                if current_user.is_authenticated:
                    abort(403, message)
                abort(401, message)
            try:
                query = query_cfg.query_class(**query_cfg.query_config)
                result[query_name] = query.run(**params)
            except ValueError as e:
                raise InvalidRequestInputError(e.args[0])
            except NotFoundError as e:
                return None
        return self.make_response(result)


class QueryRecordViewCount(ContentNegotiatedMethodView):
    """REST API resource providing record view count."""

    view_name = 'get_record_view_count'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryRecordViewCount, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def get_data(self, record_id, query_date=None, get_period=False):
        """Get data."""
        result = {}
        period = []
        country = {}

        try:
            if not query_date:
                params = {'record_id': record_id,
                          'interval': 'month'}
            else:
                year = int(query_date[0: 4])
                month = int(query_date[5: 7])
                _, lastday = calendar.monthrange(year, month)
                params = {'record_id': record_id,
                          'interval': 'month',
                          'start_date': query_date + '-01',
                          'end_date': query_date + '-' + str(lastday).zfill(2)
                          + 'T23:59:59'}
            query_period_cfg = current_stats.queries['bucket-record-view-histogram']
            query_period = query_period_cfg.query_class(
                **query_period_cfg.query_config)

            # total
            query_total_cfg = current_stats.queries['bucket-record-view-total']
            query_total = query_total_cfg.query_class(
                **query_total_cfg.query_config)
            res_total = query_total.run(**params)
            result['total'] = res_total['count']
            for d in res_total['buckets']:
                country[d['key']] = d['count']
            result['country'] = country
            # period
            if get_period:
                provide_year = int(getattr(config, 'PROVIDE_PERIOD_YEAR'))
                sYear = datetime.now().year
                sMonth = datetime.now().month
                eYear = sYear - provide_year
                start = datetime(sYear, sMonth, 15)
                end = datetime(eYear, 1, 1)
                while end < start:
                    period.append(start.strftime('%Y-%m'))
                    start -= timedelta(days=16)
                    start = datetime(start.year, start.month, 15)
                result['period'] = period
        except Exception as e:
            current_app.logger.debug(e)
            result['total'] = 0
            result['country'] = country
            result['period'] = period

        return result

    def get(self, **kwargs):
        """Get total record view count."""
        record_id = kwargs.get('record_id')
        return self.make_response(self.get_data(record_id, get_period=True))

    def post(self, **kwargs):
        """Get record view count with date."""
        record_id = kwargs.get('record_id')
        d = request.get_json(force=False)
        if d['date'] == 'total':
            date = None
        else:
            date = d['date']
        return self.make_response(self.get_data(record_id, date))


class QueryFileStatsCount(ContentNegotiatedMethodView):
    """REST API resource providing file download/preview count."""

    view_name = 'get_file_stats_count'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryFileStatsCount, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def get_data(self, bucket_id, file_key, query_date=None, get_period=False):
        """Get data."""
        result = {}
        period = []
        country_list = []
        mapping = {}

        if not query_date:
            params = {'bucket_id': bucket_id,
                      'file_key': file_key,
                      'interval': 'month'}
        else:
            year = int(query_date[0: 4])
            month = int(query_date[5: 7])
            _, lastday = calendar.monthrange(year, month)
            params = {'bucket_id': bucket_id,
                      'file_key': file_key,
                      'interval': 'month',
                      'start_date': query_date + '-01',
                      'end_date': query_date + '-' + str(lastday).zfill(2)
                      + 'T23:59:59'}

        try:
            # file download
            query_download_total_cfg = current_stats.queries['bucket-file-download-total']
            query_download_total = query_download_total_cfg.query_class(
                **query_download_total_cfg.query_config)
            res_download_total = query_download_total.run(**params)
            # file preview
            query_preview_total_cfg = current_stats.queries['bucket-file-preview-total']
            query_preview_total = query_preview_total_cfg.query_class(
                **query_preview_total_cfg.query_config)
            res_preview_total = query_preview_total.run(**params)
            # total
            result['download_total'] = res_download_total['value']
            result['preview_total'] = res_preview_total['value']
            # country
            for d in res_download_total['buckets']:
                data = {}
                data['country'] = d['key']
                data['download_counts'] = d['value']
                data['preview_counts'] = 0
                country_list.append(data)
                mapping[d['key']] = len(country_list) - 1
            for d in res_preview_total['buckets']:
                if d['key'] in mapping:
                    country_list[mapping[d['key']]
                                 ]['preview_counts'] = d['value']
                else:
                    data = {}
                    data['country'] = d['key']
                    data['download_counts'] = 0
                    data['preview_counts'] = d['value']
                    country_list.append(data)
            result['country_list'] = country_list
            # period
            if get_period:
                provide_year = int(getattr(config, 'PROVIDE_PERIOD_YEAR'))
                sYear = datetime.now().year
                sMonth = datetime.now().month
                eYear = sYear - provide_year
                start = datetime(sYear, sMonth, 15)
                end = datetime(eYear, 1, 1)
                while end < start:
                    period.append(start.strftime('%Y-%m'))
                    start -= timedelta(days=16)
                    start = datetime(start.year, start.month, 15)
                result['period'] = period
        except Exception as e:
            current_app.logger.debug(e)
            result['download_total'] = 0
            result['preview_total'] = 0
            result['country_list'] = country_list
            result['period'] = period

        return result

    def get(self, **kwargs):
        """Get total file download/preview count."""
        bucket_id = kwargs.get('bucket_id')
        file_key = kwargs.get('file_key')
        return self.make_response(
            self.get_data(
                bucket_id,
                file_key,
                get_period=True))

    def post(self, **kwargs):
        """Get file download/preview count with date."""
        bucket_id = kwargs.get('bucket_id')
        file_key = kwargs.get('file_key')
        d = request.get_json(force=False)
        if d['date'] == 'total':
            date = None
        else:
            date = d['date']
        return self.make_response(self.get_data(bucket_id, file_key, date))


class QueryFileStatsReport(ContentNegotiatedMethodView):
    """REST API resource providing file download/preview report."""

    view_name = 'get_file_stats_report'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryFileStatsReport, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def Calculation(self, res, data_list):
        """Create response object."""
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

    def get(self, **kwargs):
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
            self.Calculation(all_res, all_list)

            # open access
            open_access_query_cfg = current_stats.queries[open_access_query_name]
            open_access = open_access_query_cfg.query_class(
                **open_access_query_cfg.query_config)
            open_access_res = open_access.run(**params)
            self.Calculation(open_access_res, open_access_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list
        result['open_access'] = open_access_list

        return self.make_response(result)


class QueryItemRegReport(ContentNegotiatedMethodView):
    """REST API resource providing item registration report."""

    view_name = 'get_item_registration_report'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryItemRegReport, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def get(self, **kwargs):
        """Get item registration report."""
        target_report = kwargs.get('target_report').title()
        start_date = datetime.strptime(kwargs.get('start_date'), '%Y-%m-%d') \
            if kwargs.get('start_date') != '0' else None
        end_date = datetime.strptime(kwargs.get('end_date'), '%Y-%m-%d') \
            if kwargs.get('end_date') != '0' else None
        unit = kwargs.get('unit').title()
        empty_date_flg = True if not start_date or not end_date else False

        query_name = 'item-create-total'
        count_keyname = 'count'
        if target_report == config.TARGET_REPORTS['Item Detail']:
            if unit == 'Item':
                query_name = 'item-detail-item-total'
            else:
                query_name = 'item-detail-total' \
                    if not empty_date_flg or unit == 'Host' \
                    else 'bucket-item-detail-view-histogram'
        elif empty_date_flg:
            query_name = 'item-create-histogram'

        # total
        query_total_cfg = current_stats.queries[query_name]
        query_total = query_total_cfg.query_class(
            **query_total_cfg.query_config)

        d = start_date

        total_results = 0
        reports_per_page = int(getattr(config, 'REPORTS_PER_PAGE'))
        # get page_index from request params
        page_index = 0
        try:
            page_index = int(request.args.get('p', 1)) - 1
        except Exception as e:
            current_app.logger.debug(e)
        result = []
        if empty_date_flg or end_date >= start_date:
            try:
                if unit == 'Day':
                    if empty_date_flg:
                        params = {'interval': 'day'}
                        res_total = query_total.run(**params)
                        # Get valuable items
                        items = []
                        for item in res_total['buckets']:
                            date = item['date'].split('T')[0]
                            if item['value'] > 0 \
                                    and (not start_date or date >= start_date.strftime('%Y-%m-%d')) \
                                    and (not end_date or date <= end_date.strftime('%Y-%m-%d')):
                                items.append(item)
                        # total results
                        total_results = len(items)
                        i = 0
                        for item in items:
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                date = item['date'].split('T')[0]
                                result.append({
                                    'count': item['value'],
                                    'start_date': date,
                                    'end_date': date,
                                })
                            i += 1
                    else:
                        # total results
                        total_results = (end_date - start_date).days + 1
                        delta = timedelta(days=1)
                        for i in range(total_results):
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                start_date_string = d.strftime('%Y-%m-%d')
                                end_date_string = d.strftime('%Y-%m-%d')
                                params = {'interval': 'day',
                                          'start_date': start_date_string,
                                          'end_date': end_date_string
                                          }
                                res_total = query_total.run(**params)
                                result.append({
                                    'count': res_total[count_keyname],
                                    'start_date': start_date_string,
                                    'end_date': end_date_string,
                                })
                            d += delta
                elif unit == 'Week':
                    delta = timedelta(days=7)
                    delta1 = timedelta(days=1)
                    if empty_date_flg:
                        params = {'interval': 'week'}
                        res_total = query_total.run(**params)
                        # Get valuable items
                        items = []
                        for item in res_total['buckets']:
                            date = item['date'].split('T')[0]
                            if item['value'] > 0 \
                                    and (not start_date or date >= start_date.strftime('%Y-%m-%d')) \
                                    and (not end_date or date <= end_date.strftime('%Y-%m-%d')):
                                items.append(item)
                        # total results
                        total_results = len(items)
                        i = 0
                        import pytz
                        for item in items:
                            if item == items[0]:
                                # Start date of data
                                d = parser.parse(item['date'])

                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                start_date_string = d.strftime('%Y-%m-%d')
                                d1 = d + delta - delta1
                                if end_date and d1 > end_date.replace(
                                        tzinfo=pytz.UTC):
                                    d1 = end_date
                                end_date_string = d1.strftime('%Y-%m-%d')
                                result.append({
                                    'count': item['value'],
                                    'start_date': start_date_string,
                                    'end_date': end_date_string,
                                })
                            d += delta
                            i += 1
                    else:
                        # total results
                        total_results = int(
                            (end_date - start_date).days / 7) + 1

                        d = start_date
                        for i in range(total_results):
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                start_date_string = d.strftime('%Y-%m-%d')
                                d1 = d + delta - delta1
                                if d1 > end_date:
                                    d1 = end_date
                                end_date_string = d1.strftime('%Y-%m-%d')
                                temp = {
                                    'start_date': start_date_string,
                                    'end_date': end_date_string
                                }
                                params = {'interval': 'week',
                                          'start_date': temp['start_date'],
                                          'end_date': temp['end_date']
                                          }
                                res_total = query_total.run(**params)
                                temp['count'] = res_total[count_keyname]
                                result.append(temp)

                            d += delta
                elif unit == 'Year':
                    if empty_date_flg:
                        params = {'interval': 'year'}
                        res_total = query_total.run(**params)
                        # Get start day and end day
                        start_date_string = '{}-01-01'.format(
                            start_date.year) if start_date else None
                        end_date_string = '{}-12-31'.format(
                            end_date.year) if end_date else None
                        # Get valuable items
                        items = []
                        for item in res_total['buckets']:
                            date = item['date'].split('T')[0]
                            if item['value'] > 0 \
                                    and (not start_date_string or date >= start_date_string) \
                                    and (not end_date_string or date <= end_date_string):
                                items.append(item)
                        # total results
                        total_results = len(items)
                        i = 0
                        for item in items:
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                event_date = parser.parse(item['date'])
                                result.append({
                                    'count': item['value'],
                                    'start_date': '{}-01-01'.format(event_date.year),
                                    'end_date': '{}-12-31'.format(event_date.year),
                                    'year': event_date.year
                                })
                            i += 1
                    else:
                        start_year = start_date.year
                        end_year = end_date.year
                        # total results
                        total_results = end_year - start_year + 1
                        for i in range(total_results):
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                start_date_string = '{}-01-01'.format(
                                    start_year + i)
                                end_date_string = '{}-12-31'.format(
                                    start_year + i)
                                params = {'interval': 'year',
                                          'start_date': start_date_string,
                                          'end_date': end_date_string
                                          }
                                res_total = query_total.run(**params)
                                result.append({
                                    'count': res_total[count_keyname],
                                    'start_date': start_date_string,
                                    'end_date': end_date_string,
                                    'year': start_year + i
                                })
                elif unit == 'Item':
                    start_date_string = ''
                    end_date_string = ''
                    params = {}
                    if start_date is not None:
                        start_date_string = start_date.strftime('%Y-%m-%d')
                        params.update({'start_date': start_date_string})
                    if end_date is not None:
                        end_date_string = end_date.strftime('%Y-%m-%d')
                        params.update({'end_date': end_date_string})
                    res_total = query_total.run(**params)
                    i = 0
                    for item in res_total['buckets']:
                        # result.append({
                        #     'item_id': item['key'],
                        #     'item_name': item['buckets'][0]['key'],
                        #     'count': item[count_keyname],
                        # })
                        pid_value = item['key']
                        for h in item['buckets']:
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                record_name = h['key'] if h['key'] != 'None' else ''
                                result.append({
                                    'col1': pid_value,
                                    'col2': record_name,
                                    'col3': h[count_keyname],
                                })
                            i += 1
                            # total results
                            total_results += 1

                elif unit == 'Host':
                    start_date_string = ''
                    end_date_string = ''
                    params = {}
                    if start_date is not None:
                        start_date_string = start_date.strftime('%Y-%m-%d')
                        params.update({'start_date': start_date_string})
                    if end_date is not None:
                        end_date_string = end_date.strftime('%Y-%m-%d')
                        params.update({'end_date': end_date_string})
                    res_total = query_total.run(**params)
                    i = 0
                    for item in res_total['buckets']:
                        for h in item['buckets']:
                            if page_index * \
                                    reports_per_page <= i < (page_index + 1) * reports_per_page:
                                hostname = h['key'] if h['key'] != 'None' else ''
                                result.append({
                                    'count': h[count_keyname],
                                    'start_date': start_date_string,
                                    'end_date': end_date_string,
                                    'domain': hostname,
                                    'ip': item['key']
                                })
                            i += 1
                            # total results
                            total_results += 1
                else:
                    result = []
            except Exception as e:
                current_app.logger.debug(e)

        response = {
            'num_page': ceil(float(total_results) / reports_per_page),
            'page': page_index + 1,
            'data': result
        }
        return self.make_response(response)


class QueryRecordViewReport(ContentNegotiatedMethodView):
    """REST API resource providing record view report."""

    view_name = 'get_record_view_report'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryRecordViewReport, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def Calculation(self, res, data_list):
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

    def get(self, **kwargs):
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
            self.Calculation(all_res, all_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list

        return self.make_response(result)


class QueryRecordViewPerIndexReport(ContentNegotiatedMethodView):
    """REST API resource providing record view per index report."""

    view_name = 'get_record_view_per_index_report'
    nested_path = 'record_index_list'
    first_level_field = 'record_index_list.index_id'
    second_level_field = 'record_index_list.index_name'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryRecordViewPerIndexReport, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def get_nested_agg(self, start_date, end_date):
        """Get nested aggregation by index id."""
        agg_query = Search(using=current_search_client,
                           index='events-stats-record-view',
                           doc_type='stats-record-view')[0:0]

        if start_date is not None and end_date is not None:
            time_range = {}
            time_range['gte'] = parser.parse(start_date).isoformat()
            time_range['lte'] = parser.parse(end_date).isoformat()
            agg_query = agg_query.filter('range', **{'timestamp': time_range})

        agg_query.aggs.bucket(self.nested_path, 'nested',
                              path=self.nested_path) \
            .bucket(self.first_level_field, 'terms',
                    field=self.first_level_field, size=0) \
            .bucket(self.second_level_field, 'terms',
                    field=self.second_level_field, size=0)
        return agg_query.execute().to_dict()

    def parse_bucket_response(self, res, date):
        """Parse raw aggregation response."""
        result = {'date': date, 'indices': []}
        buckets = res['aggregations'][self.nested_path][self.first_level_field]['buckets']
        for id_agg in buckets:
            for name_agg in id_agg[self.second_level_field]['buckets']:
                result['indices'].append({'index_name': name_agg['key'],
                                          'view_count': id_agg['doc_count']})
        return result

    def get(self, **kwargs):
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
            raw_result = self.get_nested_agg(start_date, end_date)
            result = self.parse_bucket_response(raw_result, query_month)

        except Exception as e:
            current_app.logger.debug(e)
            return {}

        return self.make_response(result)


class QueryFileUsingPerUseReport(ContentNegotiatedMethodView):
    """REST API resource providing File Using Per User report."""

    view_name = 'get_file_using_per_user_report'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryFileUsingPerUseReport, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def Calculation(self, res, data_list):
        """Create response object."""
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

    def get(self, **kwargs):
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
            self.Calculation(all_res, all_list)

        except Exception as e:
            current_app.logger.debug(e)

        result['date'] = query_month
        result['all'] = all_list

        return self.make_response(result)


class QueryCeleryTaskReport(ContentNegotiatedMethodView):
    """REST API resource providing celery task report."""

    view_name = 'get_celery_task_report'

    def __init__(self, **kwargs):
        """Constructor."""
        super(QueryCeleryTaskReport, self).__init__(
            serializers={
                'application/json':
                lambda data, *args, **kwargs: jsonify(data),
            },
            default_method_media_type={
                'GET': 'application/json',
            },
            default_media_type='application/json',
            **kwargs)

    def parse_bucket_response(self, raw_res, pretty_result):
        """Parsing bucket response."""
        if 'buckets' in raw_res:
            field_name = raw_res['field']
            value = raw_res['buckets'][0]['key']
            pretty_result[field_name] = value
            return self.parse_bucket_response(
                raw_res['buckets'][0], pretty_result)
        else:
            return pretty_result

    def get(self, **kwargs):
        """Get celery task report."""
        result = {}
        list = []
        task_name = kwargs.get('task_name')
        try:
            params = {'task_name': task_name}

            # Get exec logs in certain time frame
            query_cfg = current_stats.queries['get-celery-task-report']
            query = query_cfg.query_class(**query_cfg.query_config)
            result = query.run(**params)

            pretty_result = []
            for report in result['buckets']:
                current_report = {}
                current_report['task_id'] = report['key']
                pretty_report = self.parse_bucket_response(
                    report, current_report)
                pretty_result.append(current_report)

        except Exception as e:
            current_app.logger.debug(e)
            return self.make_response([])

        return self.make_response(pretty_result)


stats_view = StatsQueryResource.as_view(
    StatsQueryResource.view_name,
)

record_view_count = QueryRecordViewCount.as_view(
    QueryRecordViewCount.view_name,
)

file_stats_count = QueryFileStatsCount.as_view(
    QueryFileStatsCount.view_name,
)

file_stats_report = QueryFileStatsReport.as_view(
    QueryFileStatsReport.view_name,
)

item_reg_report = QueryItemRegReport.as_view(
    QueryItemRegReport.view_name,
)

celery_task_report = QueryCeleryTaskReport.as_view(
    QueryCeleryTaskReport.view_name,
)

record_view_report = QueryRecordViewReport.as_view(
    QueryRecordViewReport.view_name,
)

record_view_per_index_report = QueryRecordViewPerIndexReport.as_view(
    QueryRecordViewPerIndexReport.view_name,
)

file_using_per_user_report = QueryFileUsingPerUseReport.as_view(
    QueryFileUsingPerUseReport.view_name,
)

blueprint.add_url_rule(
    '',
    view_func=stats_view,
)

blueprint.add_url_rule(
    '/<string:record_id>',
    view_func=record_view_count,
)

blueprint.add_url_rule(
    '/<string:bucket_id>/<string:file_key>',
    view_func=file_stats_count,
)

blueprint.add_url_rule(
    '/<string:event>/<int:year>/<int:month>',
    view_func=file_stats_report,
)

blueprint.add_url_rule(
    '/<string:target_report>/<string:start_date>/<string:end_date>/<string:unit>',
    view_func=item_reg_report,
)

blueprint.add_url_rule(
    '/tasks/<string:task_name>',
    view_func=celery_task_report,
)

blueprint.add_url_rule(
    '/report/record/record_view/<int:year>/<int:month>',
    view_func=record_view_report,
)

blueprint.add_url_rule(
    '/report/record/record_view_per_index/<int:year>/<int:month>',
    view_func=record_view_per_index_report,
)

blueprint.add_url_rule(
    '/report/file/<string:event>/<int:year>/<int:month>',
    view_func=file_using_per_user_report,
)
