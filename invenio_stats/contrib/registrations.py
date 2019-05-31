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
from invenio_stats.contrib.event_builders import build_celery_task_unique_id, \
    build_file_unique_id, build_item_create_unique_id, \
    build_record_unique_id, build_search_detail_condition, \
    build_search_unique_id, build_top_unique_id, copy_record_index_list
from invenio_stats.processors import EventsIndexer, anonymize_user, flag_robots
from invenio_stats.queries import ESDateHistogramQuery, ESTermsQuery


def register_events():
    """Register sample events."""
    return [
        dict(
            event_type='celery-task',
            templates='invenio_stats.contrib.celery_task',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_celery_task_unique_id
                ])),
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
            event_type='item-create',
            templates='invenio_stats.contrib.item_create',
            processor_class=EventsIndexer,
            processor_config=dict(
                preprocessors=[
                    flag_robots,
                    anonymize_user,
                    build_item_create_unique_id
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
                ]))
    ]


def register_aggregations():
    """Register sample aggregations."""
    return [dict(
        aggregation_name='celery-task-agg',
        templates='invenio_stats.contrib.aggregations.aggr_celery_task',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='celery-task',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                task_id='task_id',
                task_name='task_name',
                task_state='task_state',
                start_time='start_time',
                end_time='end_time',
                total_records='total_records',
                repository_name='repository_name',
                execution_time='execution_time',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )), dict(
        aggregation_name='file-download-agg',
        templates='invenio_stats.contrib.aggregations.aggr_file_download',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='file-download',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                country='country',
                item_id='item_id',
                item_title='item_title',
                file_key='file_key',
                bucket_id='bucket_id',
                file_id='file_id',
                accessrole='accessrole',
                userrole='userrole',
                index_list='index_list',
                site_license_flag='site_license_flag',
                cur_user_id='cur_user_id',
                hostname='hostname',
                remote_addr='remote_addr',
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
                country='country',
                item_id='item_id',
                item_title='item_title',
                file_key='file_key',
                bucket_id='bucket_id',
                file_id='file_id',
                accessrole='accessrole',
                userrole='userrole',
                index_list='index_list',
                site_license_flag='site_license_flag',
                cur_user_id='cur_user_id',
                hostname='hostname',
                remote_addr='remote_addr',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
                'volume': ('sum', 'size', {}),
            },
        )), dict(
        aggregation_name='item-create-agg',
        templates='invenio_stats.contrib.aggregations.aggr_item_create',
        aggregator_class=StatAggregator,
        aggregator_config=dict(
            client=current_search_client,
            event='item-create',
            aggregation_field='unique_id',
            aggregation_interval='day',
            copy_fields=dict(
                country='country',
                hostname='hostname',
                remote_addr='remote_addr',
                pid_type='pid_type',
                pid_value='pid_value',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
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
                country='country',
                hostname='hostname',
                remote_addr='remote_addr',
                record_id='record_id',
                record_name='record_name',
                record_index_names=copy_record_index_list,
                pid_type='pid_type',
                pid_value='pid_value',
                cur_user_id='cur_user_id',
            ),
            metric_aggregation_fields={
                'unique_count': ('cardinality', 'unique_session_id',
                                 {'precision_threshold': 1000}),
            },
        ))]


def register_queries():
    """Register queries."""
    return [
        dict(
            query_name='get-celery-task-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-celery-task',
                doc_type='celery-task-day-aggregation',
                aggregated_fields=['task_id', 'task_name', 'start_time',
                                   'end_time', 'total_records', 'task_state'],
                required_filters=dict(
                    task_name='task_name',
                )
            )
        ),
        dict(
            query_name='get-file-download-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                aggregated_fields=['file_key', 'index_list',
                                   'userrole', 'site_license_flag']
            )
        ),
        dict(
            query_name='get-file-download-open-access-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                aggregated_fields=['file_key', 'index_list',
                                   'userrole', 'site_license_flag'],
                required_filters=dict(
                    accessrole='accessrole',
                )
            )
        ),
        dict(
            query_name='get-file-preview-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-preview',
                doc_type='file-preview-day-aggregation',
                aggregated_fields=['file_key', 'index_list',
                                   'userrole', 'site_license_flag']
            )
        ),
        dict(
            query_name='get-file-preview-open-access-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-preview',
                doc_type='file-preview-day-aggregation',
                aggregated_fields=['file_key', 'index_list',
                                   'userrole', 'site_license_flag'],
                required_filters=dict(
                    accessrole='accessrole',
                )
            )
        ),
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
                    # bucket_id='bucket_id',
                ),
                required_filters=dict(
                    bucket_id='bucket_id',
                    file_key='file_key',
                ),
                aggregated_fields=['country']
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
                    file_key='file_key',
                ),
                aggregated_fields=['country']
            )
        ),
        dict(
            query_name='get-file-download-per-user-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                aggregated_fields=['cur_user_id', 'file_id']
            )
        ),
        dict(
            query_name='get-file-preview-per-user-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-preview',
                doc_type='file-preview-day-aggregation',
                aggregated_fields=['cur_user_id', 'file_id']
            )
        ),
        dict(
            query_name='get-record-view-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                aggregated_fields=['record_id', 'record_index_names',
                                   'cur_user_id']
            )
        ),
        dict(
            query_name='bucket-record-view-histogram',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                copy_fields=dict(
                    record_id='record_id',
                ),
                required_filters=dict(
                    record_id='record_id',
                )
            )
        ),
        dict(
            query_name='bucket-record-view-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                copy_fields=dict(
                    record_id='record_id',
                ),
                required_filters=dict(
                    record_id='record_id',
                ),
                metric_fields=dict(
                    count=('sum', 'count', {}),
                    unique_count=('sum', 'unique_count', {}),
                ),
                aggregated_fields=['country']
            )
        ),
        dict(
            query_name='item-create-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-item-create',
                doc_type='item-create-day-aggregation',
                metric_fields=dict(
                    count=('sum', 'count', {}),
                ),
                aggregated_fields=['remote_addr', 'hostname']
            )
        ),
        dict(
            query_name='item-create-histogram',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-item-create',
                doc_type='item-create-day-aggregation',
                aggregated_fields=['timestamp']
            )
        ),
        dict(
            query_name='item-detail-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                metric_fields=dict(
                    count=('sum', 'count', {}),
                ),
                aggregated_fields=['remote_addr', 'hostname']
            )
        ),
        dict(
            query_name='get-file-download-per-host-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                metric_fields=dict(
                    count=('sum', 'count', {}),
                ),
                aggregated_fields=['remote_addr', 'hostname']
            )
        ),
        # For query item details (id, name, count)
        dict(
            query_name='item-detail-item-total',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                metric_fields=dict(
                    count=('sum', 'count', {}),
                ),
                aggregated_fields=['pid_value', 'record_name']
            )
        ),
        dict(
            query_name='get-file-download-per-item-report',
            query_class=ESTermsQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                metric_fields=dict(
                    count=('sum', 'count', {}),
                ),
                aggregated_fields=['item_id', 'item_title']
            )
        ),
        dict(
            query_name='bucket-item-detail-view-histogram',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-record-view',
                doc_type='record-view-day-aggregation',
                aggregated_fields=['timestamp']
            )
        ),
        dict(
            query_name='get-file-download-per-time-report',
            query_class=ESDateHistogramQuery,
            query_config=dict(
                index='stats-file-download',
                doc_type='file-download-day-aggregation',
                aggregated_fields=['timestamp']
            )
        ),
    ]
