from __future__ import annotations

import time

import pytest
import polars as pl
import spice


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_async')
async def test_query_async(cache: bool):
    await spice.async_query(4388, cache=cache)


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_limits_async')
async def test_query_limits_async(cache: bool):
    df10 = await spice.async_query(4388, limit=10, cache=cache)
    assert len(df10) == 10
    df100 = await spice.async_query(4388, limit=100, cache=cache)
    assert len(df100) == 100
    df1000 = await spice.async_query(4388, limit=1000, cache=cache)
    assert len(df1000) == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_offsets_async')
async def test_query_offsets_async(cache: bool):
    df = await spice.async_query(3606949, limit=30, offset=0, cache=cache)
    df1 = await spice.async_query(3606949, limit=10, offset=0, cache=cache)
    df2 = await spice.async_query(3606949, limit=10, offset=10, cache=cache)
    df3 = await spice.async_query(3606949, limit=10, offset=20, cache=cache)
    assert df.equals(pl.concat([df1, df2, df3]))


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_sort_async')
async def test_query_sort_async(cache: bool):
    df_sort_project = await spice.async_query(
        4388, sort_by='project', cache=cache
    )
    assert df_sort_project.equals(
        df_sort_project.sort('project', nulls_last=True)
    )

    df_sort_usd_volume = await spice.async_query(
        4388, sort_by='usd_volume', cache=cache
    )
    assert df_sort_usd_volume.equals(
        df_sort_usd_volume.sort('usd_volume', nulls_last=True)
    )

    assert not df_sort_project.equals(df_sort_usd_volume)


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_columns_async')
async def test_query_columns_async(cache: bool):
    columns = ['project', '_col1']
    df_columns = await spice.async_query(4388, columns=columns, cache=cache)
    assert df_columns.columns == columns
    df = await spice.async_query(4388, cache=cache)
    assert df.columns != columns


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_types_async')
async def test_query_types_async(cache: bool):
    types = [pl.String, pl.Datetime, pl.Float32]
    df = await spice.async_query(4388, types=types, cache=cache)
    assert list(df.schema.values()) == types


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_query_without_polling_async')
async def test_query_without_polling_async(cache: bool):
    execution = await spice.async_query(
        4388, poll=False, refresh=True, cache=cache
    )
    df_no_poll = await spice.async_query(execution, cache=cache)
    df_poll = await spice.async_query(4388, cache=cache)
    assert df_no_poll.equals(df_poll)


@pytest.mark.asyncio
@pytest.mark.parametrize('cache', [False, True, True])
@pytest.mark.xdist_group(name='test_parameters_async')
async def test_parameters_async(cache: bool):
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
        df = await spice.async_query(
            1215383, parameters=parameters, cache=cache
        )
        actual_value = df.to_dicts()[0]
        assert actual_value == {
            'text_field': parameters['TextField'],
            'number_field': parameters['NumberField'],
            'date_field': parameters['DateField'],
            'list_field': parameters['ListField'],
        }
