# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""InvenioStats views."""

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


class QueryRecordViewCount(ContentNegotiatedMethodView):

    view_name = 'get_record_view_count'

    def __init__(self, **kwargs):
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

    def get(self, **kwargs):
        result = {}
        period = {}

        record_id = kwargs.get('record_id')
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
            for bucket in res_period['buckets']:
                period[bucket['date'][0:7]] = bucket['value']
            result['period'] = period
        except ValueError as e:
            raise InvalidRequestInputError(e.args[0])
        except NotFoundError as e:
            return None

        return self.make_response(result)


stats_view = StatsQueryResource.as_view(
    StatsQueryResource.view_name,
)

record_view_count = QueryRecordViewCount.as_view(
    QueryRecordViewCount.view_name,
)

blueprint.add_url_rule(
    '',
    view_func=stats_view,
)

blueprint.add_url_rule(
    '/GetRecordViewCount/<string:record_id>',
    view_func=record_view_count,
)
