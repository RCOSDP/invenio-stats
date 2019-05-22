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
import dateutil.relativedelta as relativedelta
from math import ceil

from elasticsearch.exceptions import NotFoundError
from flask import Blueprint, abort, current_app, jsonify, request
from invenio_rest.views import ContentNegotiatedMethodView

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
                    country_list[mapping[d['key']]]['preview_counts'] = d['value']
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
        start_date = datetime.strptime(kwargs.get('start_date'), '%Y-%m-%d')
        end_date = datetime.strptime(kwargs.get('end_date'), '%Y-%m-%d')
        unit = kwargs.get('unit').title()

        query_name = 'item-create-total'
        count_keyname = 'count'
        if target_report == config.TARGET_REPORTS['Item Detail']:
            if unit == 'Item':
                query_name = 'item-detail-item-total'
            else:
                query_name = 'item-detail-total'

        # total
        query_total_cfg = current_stats.queries[query_name]
        query_total = query_total_cfg.query_class(**query_total_cfg.query_config)

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
        if end_date >= start_date:
            try:
                if unit == 'Day':
                    # total results
                    total_results = (end_date - start_date).days + 1
                    delta = timedelta(days=1)
                    for i in range(total_results):
                        if page_index * reports_per_page <= i < (page_index + 1) * reports_per_page:
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
                    # Find Sunday of end_date
                    end_sunday = end_date + relativedelta.relativedelta(weekday=relativedelta.SU(+1))
                    # Find current Mon and Sun of start_date
                    current_monday = start_date + relativedelta.relativedelta(weekday=relativedelta.MO(-1))
                    current_sunday = start_date + relativedelta.relativedelta(weekday=relativedelta.SU(+1))
                    # total results
                    total_results = int((end_sunday - current_sunday).days / 7) + 1

                    delta = timedelta(days=7)
                    for i in range(total_results):
                        if page_index * reports_per_page <= i < (page_index + 1) * reports_per_page:
                            start_date_string = current_monday.strftime('%Y-%m-%d')
                            end_date_string = current_sunday.strftime('%Y-%m-%d')
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

                        current_monday += delta
                        current_sunday += delta
                elif unit == 'Year':
                    start_year = start_date.year
                    end_year = end_date.year
                    # total results
                    total_results = end_year - start_year + 1
                    for i in range(total_results):
                        if page_index * reports_per_page <= i < (page_index + 1) * reports_per_page:
                            start_date_string = '{}-01-01'.format(start_year + i)
                            end_date_string = '{}-12-31'.format(start_year + i)
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
                    start_date_string = start_date.strftime('%Y-%m-%d')
                    end_date_string = end_date.strftime('%Y-%m-%d')
                    params = {
                              'start_date': start_date_string,
                              'end_date': end_date_string
                              }
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
                            if page_index * reports_per_page <= i < (page_index + 1) * reports_per_page:
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
                    result = []
                    start_date_string = start_date.strftime('%Y-%m-%d')
                    end_date_string = end_date.strftime('%Y-%m-%d')
                    params = {
                              'start_date': start_date_string,
                              'end_date': end_date_string
                              }
                    res_total = query_total.run(**params)
                    i = 0
                    for item in res_total['buckets']:
                        for h in item['buckets']:
                            if page_index * reports_per_page <= i < (page_index + 1) * reports_per_page:
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
            'num_page': ceil(float(total_results)/reports_per_page),
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

record_view_report = QueryRecordViewReport.as_view(
    QueryRecordViewReport.view_name,
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
    '/report/record/record_view/<int:year>/<int:month>',
    view_func=record_view_report,
)

blueprint.add_url_rule(
    '/report/file/<string:event>/<int:year>/<int:month>',
    view_func=file_using_per_user_report,
)
