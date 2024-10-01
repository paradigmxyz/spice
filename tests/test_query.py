from __future__ import annotations

import time

import polars as pl
import pytest
import spice

# put into xdist_group's so that they cannot but run in parallel to test caching


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query')
def test_query(cache: bool):
    spice.query(4388, cache=cache, cache_dir='/tmp/dune_spice/test_query')


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_limits')
def test_query_limits(cache: bool):
    df10 = spice.query(
        4388,
        limit=10,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_limits',
    )
    assert len(df10) == 10
    df100 = spice.query(
        4388,
        limit=100,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_limits',
    )
    assert len(df100) == 100
    df1000 = spice.query(
        4388,
        limit=1000,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_limits',
    )
    assert len(df1000) == 1000


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_offsets')
def test_query_offsets(cache: bool):
    df = spice.query(
        3606949,
        limit=30,
        offset=0,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_offsets',
    )
    df1 = spice.query(
        3606949,
        limit=10,
        offset=0,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_offsets',
    )
    df2 = spice.query(
        3606949,
        limit=10,
        offset=10,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_offsets',
    )
    df3 = spice.query(
        3606949,
        limit=10,
        offset=20,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_offsets',
    )
    assert df.equals(pl.concat([df1, df2, df3]))


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_sort')
def test_query_sort(cache: bool):
    df_sort_project = spice.query(
        4388,
        sort_by='project',
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_sort',
    )
    assert df_sort_project.equals(
        df_sort_project.sort('project', nulls_last=True)
    )

    df_sort_usd_volume = spice.query(
        4388,
        sort_by='usd_volume',
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_sort',
    )
    assert df_sort_usd_volume.equals(
        df_sort_usd_volume.sort('usd_volume', nulls_last=True)
    )

    assert not df_sort_project.equals(df_sort_usd_volume)


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_columns')
def test_query_columns(cache: bool):
    columns = ['project', '_col1']
    df_columns = spice.query(
        4388,
        columns=columns,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_columns',
    )
    assert df_columns.columns == columns
    df = spice.query(
        4388, cache=cache, cache_dir='/tmp/dune_spice/test_query_columns'
    )
    assert df.columns != columns


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_types')
def test_query_types(cache: bool):
    types = [pl.String, pl.Datetime, pl.Float32]
    df = spice.query(
        4388,
        types=types,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_types',
    )
    assert list(df.schema.values()) == types


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_without_polling')
def test_query_without_polling(cache: bool):
    execution = spice.query(4388, poll=False, refresh=True)
    df_no_poll = spice.query(execution)
    df_poll = spice.query(
        4388,
        cache=cache,
        cache_dir='/tmp/dune_spice/test_query_without_polling',
    )
    assert df_no_poll.equals(df_poll)


@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_parameters')
def test_parameters(cache: bool):
    parameter_sets = [
        {
            'TextField': 'Word2',
            'NumberField': 5.1415926535,
            'DateField': '2022-05-04 00:00:00',
            'ListField': 'Option 2',
        },
        {
            'TextField': 'Word6',
            'NumberField': 9.1415926535,
            'DateField': '2022-05-04 00:00:00',
            'ListField': 'Option 1',
        },
        {
            'TextField': 'Word99',
            'NumberField': time.time(),
            'DateField': '2022-05-04 00:00:00',
            'ListField': 'Option 1',
        },
    ]
    for parameters in parameter_sets:
        df = spice.query(
            1215383,
            parameters=parameters,
            cache=cache,
            cache_dir='/tmp/dune_spice/test_parameters',
        )
        actual_value = df.to_dicts()[0]
        assert actual_value == {
            'text_field': parameters['TextField'],
            'number_field': parameters['NumberField'],
            'date_field': parameters['DateField'],
            'list_field': parameters['ListField'],
        }
