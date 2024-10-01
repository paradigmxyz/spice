from __future__ import annotations

import os
import typing

import spice
from . import _extract

if typing.TYPE_CHECKING:
    import polars as pl
    from ._types import Execution


cache_template = (
    '{query_id}__{execution_id}__{parameter_hash}__{timestamp}.parquet'
)


def load_from_cache(
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.RetrievalKwargs,
    output_kwargs: _extract.OutputKwargs,
) -> tuple[
    pl.DataFrame | tuple[pl.DataFrame, Execution] | None, Execution | None
]:
    # get latest execution id
    execution = _extract.get_latest_execution(execute_kwargs)
    if execution is None:
        return None, None

    # build cache path
    cache_path = _build_cache_path(
        execution=execution,
        execute_kwargs=execute_kwargs,
        result_kwargs=result_kwargs,
        cache_dir=output_kwargs['cache_dir'],
    )

    # load cache result
    if os.path.exists(cache_path):
        import polars as pl

        if result_kwargs['verbose']:
            print('loading result from cache')
        df = pl.read_parquet(cache_path)
        if output_kwargs['include_execution']:
            return (df, execution), execution
        else:
            return df, execution
    else:
        return None, execution


async def async_load_from_cache(
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.RetrievalKwargs,
    output_kwargs: _extract.OutputKwargs,
) -> tuple[
    pl.DataFrame | tuple[pl.DataFrame, Execution] | None, Execution | None
]:
    # get latest execution
    execution = await _extract.async_get_latest_execution(execute_kwargs)
    if execution is None:
        return None, None

    # build cache path
    cache_path = _build_cache_path(
        execution=execution,
        execute_kwargs=execute_kwargs,
        result_kwargs=result_kwargs,
        cache_dir=output_kwargs['cache_dir'],
    )

    # load cache result
    if os.path.exists(cache_path):
        import polars as pl

        if result_kwargs['verbose']:
            print('loading result from cache')
        df = await pl.scan_parquet(cache_path).collect_async()
        if output_kwargs['include_execution']:
            return (df, execution), execution
        else:
            return df, execution
    else:
        return None, execution


def save_to_cache(
    df: pl.DataFrame,
    execution: Execution,
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.RetrievalKwargs,
    cache_dir: str | None,
) -> None:
    import secrets
    import shutil

    if result_kwargs['verbose']:
        print('saving result to cache')

    # build cache path
    cache_path = _build_cache_path(
        execution=execution,
        execute_kwargs=execute_kwargs,
        result_kwargs=result_kwargs,
        cache_dir=cache_dir,
    )

    # create dir
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    # save to cache
    tmp_path = (
        cache_path + '_tmp_' + secrets.token_hex(8)
    )  # add for if running in parallel
    df.write_parquet(tmp_path)
    shutil.move(tmp_path, cache_path)


def _preserialize_types(
    result_kwargs: _extract.RetrievalKwargs,
    key: str,
) -> list[str | list[str]] | None:
    raw = result_kwargs[key]  # type: ignore
    if raw is None:
        types: list[str | list[str]] | None = None
    else:
        types = []
        if isinstance(raw, list):
            for type in raw:
                types.append(str(type))
        elif isinstance(raw, dict):
            for name, type in raw.items():
                types.append([name, str(type)])
        else:
            raise Exception('invalid format for ' + key)
    return types


def _build_cache_path(
    execution: Execution,
    execute_kwargs: _extract.ExecuteKwargs,
    result_kwargs: _extract.RetrievalKwargs,
    cache_dir: str | None,
) -> str:
    import hashlib
    import json

    # get parameter hash
    types = _preserialize_types(result_kwargs, 'types')
    all_types = _preserialize_types(result_kwargs, 'all_types')
    hash_params = {
        'spice_version': spice.__version__,
        'execution_id': execution['execution_id'],
        'query_id': execute_kwargs['query_id'],
        'parameters': execute_kwargs['parameters'],
        'limit': result_kwargs['limit'],
        'offset': result_kwargs['offset'],
        'sample_count': result_kwargs['sample_count'],
        'sort_by': result_kwargs['sort_by'],
        'columns': result_kwargs['columns'],
        'extras': result_kwargs['extras'],
        'types': types,
        'all_types': all_types,
    }
    md5_hash = hashlib.md5()
    md5_hash.update(json.dumps(hash_params, sort_keys=True).encode('utf-8'))
    hash_str = md5_hash.hexdigest()[:16]

    # build from template
    timestamp = execution['timestamp']
    if timestamp is None:
        raise Exception('need completion timestamp on execution')
    cache_filename = cache_template.format(
        query_id=execute_kwargs['query_id'],
        execution_id=execution['execution_id'],
        parameter_hash=hash_str,
        timestamp=int(timestamp),
    )

    if cache_dir is None:
        cache_dir = '/tmp/dune_spice'

    return os.path.join(cache_dir, cache_filename)
