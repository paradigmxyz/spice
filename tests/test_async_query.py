from __future__ import annotations

import time

import pytest
import polars as pl
import spice


@pytest.mark.asyncio
async def test_query_async():
    await spice.async_query(4388)


@pytest.mark.asyncio
async def test_query_limits_async():
    df10 = await spice.async_query(4388, limit=10)
    assert len(df10) == 10
    df100 = await spice.async_query(4388, limit=100)
    assert len(df100) == 100
    df1000 = await spice.async_query(4388, limit=1000)
    assert len(df1000) == 1000


@pytest.mark.asyncio
async def test_query_offsets_async():
    df = await spice.async_query(4388, limit=30, offset=0)
    df1 = await spice.async_query(4388, limit=10, offset=0)
    df2 = await spice.async_query(4388, limit=10, offset=10)
    df3 = await spice.async_query(4388, limit=10, offset=20)
    assert df.equals(pl.concat([df1, df2, df3]))


@pytest.mark.asyncio
async def test_query_sort_async():
    df_sort_project = await spice.async_query(4388, sort_by='project')
    assert df_sort_project.equals(df_sort_project.sort('project', nulls_last=True))

    df_sort_usd_volume = await spice.async_query(4388, sort_by='usd_volume')
    assert df_sort_usd_volume.equals(
        df_sort_usd_volume.sort('usd_volume', nulls_last=True)
    )

    assert not df_sort_project.equals(df_sort_usd_volume)


@pytest.mark.asyncio
async def test_query_columns_async():
    columns = ['project', '_col1']
    df_columns = await spice.async_query(4388, columns=columns)
    assert df_columns.columns == columns
    df = await spice.async_query(4388)
    assert df.columns != columns


@pytest.mark.asyncio
async def test_query_dtypes_async():
    dtypes = [pl.String, pl.Datetime, pl.Float32]
    df = await spice.async_query(4388, dtypes=dtypes)
    assert list(df.schema.values()) == dtypes


@pytest.mark.asyncio
async def test_query_without_polling_async():
    execution = await spice.async_query(4388, poll=False, refresh=True)
    df_no_poll = await spice.async_query(execution)
    df_poll = await spice.async_query(4388)
    assert df_no_poll.equals(df_poll)


@pytest.mark.asyncio
async def test_parameters_async():
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
        df = await spice.async_query(1215383, parameters=parameters)
        actual_value = df.to_dicts()[0]
        assert actual_value == {
            'text_field': parameters['TextField'],
            'number_field': parameters['NumberField'],
            'date_field': parameters['DateField'],
            'list_field': parameters['ListField'],
        }
