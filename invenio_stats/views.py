# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""InvenioStats views."""
from datetime import datetime, timedelta
from elasticsearch.exceptions import NotFoundError
from flask import Blueprint, abort, jsonify, request
from invenio_rest.views import ContentNegotiatedMethodView

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
                    '"params": STAT_PARAMS \}}'
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

class QueryRecordViewCount():
#class QueryRecordViewCount(ContentNegotiatedMethodView):

    #view_name = 'get_record_view_count'

    #def __init__(self, **kwargs):
        #super(QueryRecordViewCount, self).__init__(
            #serializers={
            #    'application/json':
            #    lambda data, *args, **kwargs: jsonify(data),
            #},
            #default_method_media_type={
            #    'GET': 'application/json',
            #},
            #default_media_type='application/json',
            #**kwargs)

    def get_count(record_id):
        result = {}
        period = {}

        #record_id = kwargs.get('record_id')

        params_total = {'record_id': record_id}
        params_period = {'record_id': record_id, 'interval': 'month'}
        query_total_cfg = current_stats.queries['bucket-record-view-total']
        query_period_cfg = current_stats.queries['bucket-record-view-histogram']
        query_total = query_total_cfg.query_class(**query_total_cfg.query_config)
        query_period = query_period_cfg.query_class(**query_period_cfg.query_config)

        try:
            res_total = query_total.run(**params_total)
            res_period = query_period.run(**params_period)
            result['total'] = res_total['count']
            for m in res_period['buckets']:
                data = {}
                data['total'] = m['value']
                data['domain'] = {'xxx.co.jp': m['value'] // 2,
                                  'yyy.com': m['value'] // 2} # test data
                period[m['date'][0:7]] = data
            result['period'] = period
            result['domain'] = {'xxx.co.jp': res_total['count'] // 2,
                                'yyy.com': res_total['count'] // 2} # test data
        #except ValueError as e:
        #    raise InvalidRequestInputError(e.args[0])
        #except NotFoundError as e:
        #    return None
        except:
            return {}

        return result#self.make_response(result)


class QueryFileStatsCount(ContentNegotiatedMethodView):

    view_name = 'get_file_stats_count'

    def __init__(self, **kwargs):
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

    def get(self, **kwargs):
        result = {}
        period = {}

        bucket_id = kwargs.get('bucket_id')
        file_key = kwargs.get('file_key')

        params_total = {'bucket_id': bucket_id, 'file_key': file_key}
        params_period = {'bucket_id': bucket_id, 'file_key': file_key, 'interval': 'month'}

        # file download
        query_download_total_cfg = current_stats.queries['bucket-file-download-total']
        query_download_period_cfg = current_stats.queries['bucket-file-download-histogram']
        query_download_total = query_download_total_cfg.query_class(**query_download_total_cfg.query_config)
        query_download_period = query_download_period_cfg.query_class(**query_download_period_cfg.query_config)

        # file preview
        query_preview_total_cfg = current_stats.queries['bucket-file-preview-total']
        query_preview_period_cfg = current_stats.queries['bucket-file-preview-histogram']
        query_preview_total = query_preview_total_cfg.query_class(**query_preview_total_cfg.query_config)
        query_preview_period = query_preview_period_cfg.query_class(**query_preview_period_cfg.query_config)

        try:
            # file download
            res_download_total = query_download_total.run(**params_total)
            res_download_period = query_download_period.run(**params_period)
            # file preview
            res_preview_total = query_preview_total.run(**params_total)
            res_preview_period = query_preview_period.run(**params_period)
            # total
            result['download_total'] = res_download_total['value']
            result['preview_total'] = res_preview_total['value']
            # period
            for m in res_download_period['buckets']:
                data = {}
                data['download_total'] = m['value']
                data['preview_total'] = 0
                data['domain_list'] = [{'domain': 'xxx.yy.jp',
                                        'download_counts':
                                        m['value'] // 2,
                                        'preview_counts': 0},
                                       {'domain': 'yyy.com',
                                        'download_counts':
                                        m['value'] // 2,
                                        'preview_counts': 0}] # test data
                period[m['date'][0:7]] = data
            for m in res_preview_period['buckets']:
                if m['date'][0:7] in period:
                    data = period[m['date'][0:7]]
                    data['preview_total'] = m['value']
                    # test data
                    data['domain_list'][0]['preview_counts'] = m['value'] // 2
                    data['domain_list'][1]['preview_counts'] = m['value'] // 2
                else:
                    data = {}
                    data['download_total'] = 0
                    data['preview_total'] = m['value']
                    data['domain_list'] = [{'domain': 'xxx.yy.jp',
                                            'download_counts': 0,
                                            'preview_counts':
                                            m['value'] // 2},
                                           {'domain': 'yyy.com',
                                            'download_counts': 0,
                                            'preview_counts':
                                            m['value'] // 2}] # test data
                period[m['date'][0:7]] = data
            result['period'] = period
            # total domain - test data
            result['domain_list'] = [{'domain': 'xxx.yy.jp',
                                 'download_counts':
                                 res_download_total['value'] // 2,
                                 'preview_counts':
                                 res_preview_total['value'] // 2},
                                {'domain': 'yyy.com',
                                 'download_counts':
                                 res_download_total['value'] // 2,
                                 'preview_counts':
                                 res_preview_total['value'] // 2}]
        except ValueError as e:
            raise InvalidRequestInputError(e.args[0])
        except NotFoundError as e:
            return None

        return self.make_response(result)


class QueryFileStatsReport(ContentNegotiatedMethodView):

    view_name = 'get_file_stats_report'

    def __init__(self, **kwargs):
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

    def get(self, **kwargs):
        result = {}
        download_list = []
        preview_list = []


        year = kwargs.get('year')
        month = kwargs.get('month')

        # test data start
        index = ['インデックス1', 'インデックス2| インデックス3',
                 'インデックス1\インデックス1-1']
        for i in range(5):
            count = {'index_list': index[i%3],
                    'file_key': 'file' + str(i) + '.pdf',
                    'total': 100 + i * 2,
                    'login': 40 + i,
                    'no_login': 60 + i,
                    'site_license': 15,
                    'admin': 20,
                    'reg': 10}
            download_list.append(count)
            if i > 2:
                preview_list.append(count)
        # test data end

        result['date'] = str(year) + '-' + str(month).zfill(2)
        result['file_download'] = download_list
        result['file_preview'] = preview_list

        return self.make_response(result)


class QueryItemRegReport(ContentNegotiatedMethodView):

    view_name = 'get_item_registration_report'

    def __init__(self, **kwargs):
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
            temp1 = {'domain':'xxx.yy.jp','ip':'10.23.56.76','counts':100}
            result.append(temp1)
            temp2 = {'domain':'xxx.yy.com','ip':'10.24.57.76','counts':130}
            result.append(temp2)
        else:
            result = {}

        return self.make_response(result)


stats_view = StatsQueryResource.as_view(
    StatsQueryResource.view_name,
)

#record_view_count = QueryRecordViewCount.as_view(
#    QueryRecordViewCount.view_name,
#)

file_stats_count = QueryFileStatsCount.as_view(
    QueryFileStatsCount.view_name,
)

file_stats_report = QueryFileStatsReport.as_view(
    QueryFileStatsReport.view_name,
)

item_reg_report = QueryItemRegReport.as_view(
    QueryItemRegReport.view_name,
)

blueprint.add_url_rule(
    '',
    view_func=stats_view,
)

#blueprint.add_url_rule(
#    '/GetRecordViewCount/<string:record_id>',
#    view_func=record_view_count,
#)

blueprint.add_url_rule(
    '/GetFileStatsCount/<string:bucket_id>/<string:file_key>',
    view_func=file_stats_count,
)

blueprint.add_url_rule(
    '/GetFileStatsReport/<int:year>/<int:month>',
    view_func=file_stats_report,
)

blueprint.add_url_rule(
    '/GetItemRegReport/<string:start_date>/<string:end_date>/<string:unit>',
    view_func=item_reg_report,
)
