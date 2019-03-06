# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2017-2018 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""Registration of contrib events."""
from invenio_search import current_search_client

from invenio_stats.aggregations import StatAggregator
from invenio_stats.contrib.event_builders import build_file_unique_id, \
    build_record_unique_id, build_top_unique_id, build_search_unique_id, \
    build_search_detail_condition
from invenio_stats.processors import EventsIndexer, anonymize_user, flag_robots
from invenio_stats.queries import ESDateHistogramQuery, ESTermsQuery

from flask_principal import ActionNeed
from invenio_access.permissions import Permission
from invenio_stats.aggregations import StatAggregator
from invenio_stats.queries import ESTermsQuery

from .proxies import current_stats_search_client


def register_events():
    """Register sample events."""
    return [
        dict(
            event_type='file-download',
            templates='invenio_stats.contrib.file_download',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_file_unique_id
                ])),
        dict(
            event_type='file-preview',
            templates='invenio_stats.contrib.file_preview',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_file_unique_id
                ])),
        dict(
            event_type='record-view',
            templates='invenio_stats.contrib.record_view',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_record_unique_id
                ])),
        dict(
            event_type='top-view',
            templates='invenio_stats.contrib.record_view',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_top_unique_id
                ])),
        dict(
            event_type='search',
            templates='invenio_stats.contrib.search',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_search_detail_condition,
                    build_search_unique_id
                ])),
        dict(
        aggregation_name='record-download-agg',
        templates='zenodo.modules.stats.templates.aggregations',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_stats_search_client,
            event='file-download',
            aggregation_field='recid',
            aggregation_interval='day',
            batch_size=1,
            copy_fields=dict(
                bucket_id='bucket_id',
                record_id='record_id',
                recid='recid',
                conceptrecid='conceptrecid',
                doi='doi',
                conceptdoi='conceptdoi',
                communities=lambda d, _: (list(d.communities)
                                          if d.communities else None),
                owners=lambda d, _: (list(d.owners) if d.owners else None),
                is_parent=lambda *_: False
            ),
            metric_aggregation_fields=dict(
                unique_count=('cardinality', 'unique_session_id',
                              {'precision_threshold': 1000}),
                volume=('sum', 'size', {}),
            )
        )),
        dict(
            aggregation_name='record-download-all-versions-agg',
            templates='zenodo.modules.stats.templates.aggregations',
            aggregator_class=StatAggregator,
            aggregator_config=dict(
                client=current_stats_search_client,
                event='file-download',
                aggregation_field='conceptrecid',
                aggregation_interval='day',
                batch_size=1,
                copy_fields=dict(
                    conceptrecid='conceptrecid',
                    conceptdoi='conceptdoi',
                    communities=lambda d, _: (list(d.communities)
                                              if d.communities else None),
                    owners=lambda d, _: (list(d.owners) if d.owners else None),
                    is_parent=lambda *_: True
                ),
                metric_aggregation_fields=dict(
                    unique_count=('cardinality', 'unique_session_id',
                                  {'precision_threshold': 1000}),
                    volume=('sum', 'size', {}),
                )
            )),
        # NOTE: Since the "record-view-agg" aggregations is already registered
        # in "invenio_stasts.contrib.registrations", we have to overwrite the
        # configuration in "zenodo.config.STATS_AGGREGATIONS".
        dict(
            aggregation_name='record-view-all-versions-agg',
            templates='zenodo.modules.stats.templates.aggregations',
            aggregator_class=StatAggregator,
            aggregator_config=dict(
                client=current_stats_search_client,
                event='record-view',
                aggregation_field='conceptrecid',
                aggregation_interval='day',
                batch_size=1,
                copy_fields=dict(
                    conceptrecid='conceptrecid',
                    conceptdoi='conceptdoi',
                    communities=lambda d, _: (list(d.communities)
                                              if d.communities else None),
                    owners=lambda d, _: (list(d.owners) if d.owners else None),
                    is_parent=lambda *_: True
                ),
                metric_aggregation_fields=dict(
                    unique_count=('cardinality', 'unique_session_id',
                                  {'precision_threshold': 1000}),
                )
            )),
    ]


def register_aggregations():
    """Register sample aggregations."""
    return [dict(
        aggregation_name='file-download-agg', # 'record-download-agg'
        templates='invenio_stats.contrib.aggregations.aggr_file_download',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='file-download',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                file_key='file_key',
                bucket_id='bucket_id',
                file_id='file_id',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )), dict(
        aggregation_name='file-preview-agg',
        templates='invenio_stats.contrib.aggregations.aggr_file_preview',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='file-preview',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                file_key='file_key',
                bucket_id='bucket_id',
                file_id='file_id',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )), dict(
        aggregation_name='record-view-agg',
        templates='invenio_stats.contrib.aggregations.aggr_record_view',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='record-view',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                record_id='record_id',
                pid_type='pid_type',
                pid_value='pid_value',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
            },
        ))]

def queries_permission_factory(query_name, params):
    """Queries permission factory."""
    return Permission(ActionNeed('admin-access'))

def register_queries():
    """Register queries."""
    return [
        dict(
            query_name='bucket-file-download-histogram',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                copy_fields=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                )
            )
        ),
        dict(
            query_name='bucket-file-download-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                copy_fields=dict(
                    bucket_id='bucket_id',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                ),
                aggregated_fields=['file_key']
            )
        ),
        dict(
            query_name='bucket-file-preview-histogram',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-file-preview',
                doc_type='file-preview-day-aggregation',
                copy_fields=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                )
            )
        ),
        dict(
            query_name='bucket-file-preview-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-preview',
                doc_type='file-preview-day-aggregation',
                copy_fields=dict(
                    # bucket_id='bucket_id',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                ),
                aggregated_fields=['file_key']
            )
        ),
        # Weko queries.
        dict(
            query_name='record-download',
            query_class=ESTermsQuery,
            permission_factory=queries_permission_factory,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                copy_fields=dict(
                    bucket_id='bucket_id',
                    record_id='record_id',
                    recid='recid',
                    conceptrecid='conceptrecid',
                    doi='doi',
                    conceptdoi='conceptdoi',
                    communities='communities',
                    owners='owners',
                    is_parent='is_parent'
                ),
                required_filters=dict(
                    recid='recid',
                ),
                metric_fields=dict(
                    count=('sum', 'count', {}),
                    unique_count=('sum', 'unique_count', {}),
                    volume=('sum', 'volume', {}),
                )
            ),
        ),
        dict(
            query_name='record-download-all-versions',
            query_class=ESTermsQuery,
            permission_factory=queries_permission_factory,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                copy_fields=dict(
                    conceptrecid='conceptrecid',
                    conceptdoi='conceptdoi',
                    communities='communities',
                    owners='owners',
                    is_parent='is_parent'
                ),
                query_modifiers=[
                    lambda query, **_: query.filter('term', is_parent=True)
                ],
                required_filters=dict(
                    conceptrecid='conceptrecid',
                ),
                metric_fields=dict(
                    count=('sum', 'count', {}),
                    unique_count=('sum', 'unique_count', {}),
                    volume=('sum', 'volume', {}),
                )
            )
        ),
        dict(
            query_name='record-view',
            query_class=ESTermsQuery,
            permission_factory=queries_permission_factory,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                copy_fields=dict(
                    record_id='record_id',
                    recid='recid',
                    conceptrecid='conceptrecid',
                    doi='doi',
                    conceptdoi='conceptdoi',
                    communities='communities',
                    owners='owners',
                    is_parent='is_parent'
                ),
                required_filters=dict(
                    recid='recid',
                ),
                metric_fields=dict(
                    count=('sum', 'count', {}),
                    unique_count=('sum', 'unique_count', {}),
                )
            )
        ),
        dict(
            query_name='record-view-all-versions',
            query_class=ESTermsQuery,
            permission_factory=queries_permission_factory,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                copy_fields=dict(
                    conceptrecid='conceptrecid',
                    conceptdoi='conceptdoi',
                    communities='communities',
                    owners='owners',
                    is_parent='is_parent'
                ),
                query_modifiers=[
                    lambda query, **_: query.filter('term', is_parent=True)
                ],
                required_filters=dict(
                    conceptrecid='conceptrecid',
                ),
                metric_fields=dict(
                    count=('sum', 'count', {}),
                    unique_count=('sum', 'unique_count', {}),
                )
            )
        ),
    ]
