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
        start_date = datetime.strptime(kwargs.get('start_date'), '%Y-%m-%d')
        end_date = datetime.strptime(kwargs.get('end_date'), '%Y-%m-%d')
        unit = kwargs.get('unit').title()

        d = start_date
        if unit == 'Day':
            result = {}
            delta = timedelta(days=1)
            while d <= end_date:
                result[d.strftime('%Y-%m-%d')] = 10
                d += delta
        elif unit == 'Week':
            result = []
            delta = timedelta(days=7)
            d1 = timedelta(days=1)
            while d <= end_date:
                temp = {}
                temp['start_date'] = d.strftime('%Y-%m-%d')
                d += delta
                t = d - d1
                temp['end_date'] = t.strftime('%Y-%m-%d')
                temp['counts'] = 10
                result.append(temp)
        elif unit == 'Year':
            result = {}
            start_year = start_date.year
            end_year = end_date.year
            for i in range(end_year - start_year + 1):
                result[start_year + i] = 20 + i
        elif unit == 'Host':
            result = []
            temp1 = {'domain': 'xxx.yy.jp', 'ip': '10.23.56.76', 'counts': 100}
            result.append(temp1)
            temp2 = {
                'domain': 'xxx.yy.com',
                'ip': '10.24.57.76',
                'counts': 130}
            result.append(temp2)
        else:
            result = {}

        return self.make_response(result)


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
        for record in res['buckets']:
            data = {}
            data['record_id'] = record['key']
            #data['index_list'] = index['key']
            data['total'] = record['value']
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
