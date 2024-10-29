from __future__ import annotations

import argparse
from typing import Any, Mapping, Literal, TYPE_CHECKING

import rich_argparse

import spice

if TYPE_CHECKING:
    import typing_extensions
    import polars as pl


class SpiceHelpFormatter(rich_argparse.RichHelpFormatter):
    usage_markup = True

    styles = {
        'argparse.prog': 'bold rgb(197,149,242)',
        'argparse.groups': 'bold dark_orange',
        'argparse.args': 'rgb(197,149,242)',
        'argparse.metavar': 'rgb(253,240,236)',
        'argparse.help': 'grey62',
        'argparse.text': 'blue',
        'argparse.syntax': 'blue',
        'argparse.default': 'blue',
    }

    def __init__(self, prog: str) -> None:
        super().__init__('spice', max_help_position=36)

    def _format_args(self, action, default_metavar):  # type: ignore
        get_metavar = self._metavar_formatter(action, default_metavar)
        if action.nargs == argparse.ZERO_OR_MORE:
            return '[%s [%s ...]]' % get_metavar(2)
        elif action.nargs == argparse.ONE_OR_MORE:
            return '%s [...]' % get_metavar(1)
        return super()._format_args(action, default_metavar)

    def format_help(self) -> str:
        import rich

        rich.print(
            '[bold rgb(197,149,242)]spice[/bold rgb(197,149,242)] 🌶️  [grey62]is a cli tool for extracting data from the [bold][dark_orange]Dune[/dark_orange][/bold] API[/grey62]\n'
        )

        # indent certain arguments for full alignment
        lines = super().format_help().split('\n')
        for i, line in enumerate(lines):
            if line.startswith('  \x1b[38;2;197;149;242m--'):
                lines[i] = '    ' + lines[i].replace('    ', '', 1)
        lines = [
            line
            for line in lines
            if 'Options:' not in line and '--help' not in line
        ]
        return (
            '\n'.join(lines)
            .replace('\n\n\n', '\n\n')
            .replace(
                '--csv\x1b[0m                  ',
                '--csv\x1b[0m, \x1b[38;2;197;149;242m--json\x1b[0m, \x1b[38;2;197;149;242m--ndjson',
            )
        )


class SpiceArgParser(argparse.ArgumentParser):
    def error(self, message: str) -> typing_extensions.NoReturn:
        import sys
        import rich

        sys.stderr.write(f'Error: {message}\n')
        print()
        self.print_usage()
        print()
        rich.print(
            '[grey62]show all options with[/grey62] [bold white]spice -h[/bold white]'
        )
        sys.exit(0)


def parse_args() -> argparse.Namespace:
    parser = SpiceArgParser(
        formatter_class=SpiceHelpFormatter,
        usage="""%(prog)s [bold rgb(197,149,242)]QUERY [OPTIONS][/bold rgb(197,149,242)]\n\n[grey62 not bold]

This will 1. collect query data, 2. print it, and 3. save it to disk[grey62 not bold]

[bold dark_orange]Examples:[/bold dark_orange]
  [bold rgb(197,149,242)]spice 3237025[/bold rgb(197,149,242)]                                 by query id
  [bold rgb(197,149,242)]spice https://dune.com/queries/3237025[/bold rgb(197,149,242)]        by query url
  [bold rgb(197,149,242)]spice "SELECT COUNT(*) FROM ethereum.blocks"[/bold rgb(197,149,242)]  by raw sql""",
    )
    group = parser.add_argument_group('Execution Parameters')
    group.add_argument(
        'query',
        metavar='QUERY',
        help='query id, query url, or raw SQL',
        nargs='?',
    )
    group.add_argument(
        '-p',
        '--parameters',
        metavar='KEY=VALUE',
        help='SQL query parameters',
        nargs='+',
    )
    group.add_argument(
        '-r',
        '--refresh',
        help='refresh query with a new execution',
        action='store_true',
    )
    group.add_argument(
        '--max-age',
        metavar='SECONDS',
        type=float,
        help='max age allowed before re-executing',
    )
    group.add_argument(
        '--api-key', help='dune api key, default is DUNE_API_KEY'
    )
    group.add_argument(
        '--performance', help='performance level', default='medium'
    )
    group.add_argument(
        '--poll-interval',
        metavar='SECONDS',
        help='poll interval in seconds',
        type=float,
        default=1.0,
    )
    group = parser.add_argument_group('Retrieval Parameters')
    group.add_argument(
        '-l',
        '--limit',
        metavar='N_ROWS',
        help='number of rows to include in result',
        type=int,
    )
    group.add_argument(
        '--offset',
        metavar='START_ROW',
        help='row number to start returning from',
    )
    group.add_argument(
        '-s',
        '--sort-by',
        metavar='ORDER_BY_CLAUSE',
        help='an ORDER BY clause to sort data by',
    )
    group.add_argument(
        '-c',
        '--columns',
        metavar='COLUMN',
        help='columns to retrieve, default is all',
        nargs='+',
    )
    group.add_argument(
        '-t',
        '--types',
        metavar='COLUMN=DTYPE',
        help='types of columns',
        nargs='+',
    )
    group.add_argument(
        '--all-types',
        metavar='COLUMN=DTYPE',
        help='strict types of all columns',
        nargs='+',
    )
    group.add_argument(
        '--sample-count',
        metavar='COUNT',
        type=int,
        help='number random samples to return',
    )
    group.add_argument(
        '--extras',
        # help='headers for execution result',
        help=argparse.SUPPRESS,
    )
    group = parser.add_argument_group('Cache Parameters')
    group.add_argument(
        '--no-cache', help='avoid using cache', action='store_true'
    )
    group.add_argument(
        '--no-cache-load',
        help='avoid loading result from cache',
        action='store_true',
    )
    group.add_argument(
        '--no-cache-save',
        help='avoid saving result to cache',
        action='store_true',
    )
    group.add_argument(
        '--cache-dir', metavar='DIR_PATH', help='cache directory path'
    )
    group = parser.add_argument_group('Output Parameters')
    group.add_argument(
        '-v',
        '--verbose',
        metavar='LEVEL',
        help='0 = nothing, 1 = results, 2 = steps',
        default=2,
        type=int,
    )
    group.add_argument(
        '--no-save', help='do not save results to disk', action='store_true'
    )
    group.add_argument(
        '--csv', help='output as csv or json or ndjson', action='store_true'
    )
    group.add_argument(
        '--json',
        # help='output as json instead of parquet',
        help=argparse.SUPPRESS,
        action='store_true',
    )
    group.add_argument(
        '--ndjson',
        # help='output as ndjson instead of parquet',
        help=argparse.SUPPRESS,
        action='store_true',
    )
    group.add_argument(
        '-d',
        '--output-dir',
        metavar='DIR_PATH',
        help='output directory, default is CWD',
    )
    group.add_argument(
        '-f',
        '--output-file',
        metavar='FILE_PATH',
        help='output file path',
    )
    group.add_argument(
        '--query-name',
        metavar='NAME',
        help='query name to use in filename',
    )
    group.add_argument(
        '--label',
        metavar='LABEL',
        help='label to add to filename',
    )
    group.add_argument(
        '--pipe',
        help='format output for cli piping',
        action='store_true',
    )
    group.add_argument(
        '-i',
        '--interactive',
        help='open result in python session',
        action='store_true',
    )
    args = parser.parse_args()
    if args.query is None:
        import sys

        parser.print_help()
        sys.exit()
    return args


def run_cli() -> None:
    args = parse_args()
    if args.parameters is None:
        parameters = None
    else:
        parameters = dict([arg.split('=') for arg in args.parameters])
    if args.types is None:
        types = None
    else:
        types = {}
        for arg in args.types:
            key, value = arg.split('=')
            types[key] = eval(value)
    if args.all_types is None:
        all_types = None
    else:
        all_types = {}
        for arg in args.all_types:
            key, value = arg.split('=')
            all_types[key] = eval(value)

    if args.pipe:
        if args.interactive:
            raise Exception('cannot use --pipe and --interactive together')
        verbose = 0
    else:
        verbose = args.verbose

    df, execution = spice.query(  # type: ignore
        query_or_execution=args.query,
        verbose=verbose,
        refresh=args.refresh,
        max_age=args.max_age,
        parameters=parameters,
        api_key=args.api_key,
        performance=args.performance,
        poll_interval=args.poll_interval,
        limit=args.limit,
        offset=args.offset,
        sample_count=args.sample_count,
        sort_by=args.sort_by,
        columns=args.columns,
        extras=None,
        types=types,
        all_types=all_types,
        cache=not args.no_cache,
        cache_dir=args.cache_dir,
        save_to_cache=not args.no_cache_save,
        load_from_cache=not args.no_cache_load,
        include_execution=True,
    )

    format = determine_output_format(
        args.csv, args.json, args.ndjson, args.output_file
    )

    if verbose >= 2:
        print()
        print('results:')
    if verbose >= 1:
        if args.no_save and format == 'csv':
            print(df.write_csv().strip())
        elif args.no_save and format == 'json':
            print(df.write_json().strip())
        elif args.no_save and format == 'ndjson':
            print(df.write_ndjson().strip())
        else:
            print(df)
    if args.pipe:
        if format == 'csv':
            print(df.write_csv().strip())
        elif format == 'json':
            print(df.write_json().strip())
        elif format == 'ndjson':
            print(df.write_ndjson().strip())
        elif format == 'parquet':
            import sys

            df.write_parquet(sys.stdout.buffer)
        else:
            raise Exception('invalid output format: ' + str(format))

    if args.interactive:
        open_interactive_session(df)

    if not args.no_save:
        output_path = get_output_path(
            query=args.query,
            execution=execution,
            parameters=parameters,
            api_key=args.api_key,
            output_path=args.output_file,
            output_dir=args.output_dir,
            performance=args.performance,
            format=format,
            query_name=args.query_name,
            label=args.label,
        )
        if verbose >= 2:
            import rich

            print()
            rich.print('saving to [bold white]' + output_path + '[/bold white]')

        save_file(df, output_path, format)


def open_interactive_session(df: pl.DataFrame) -> None:
    header = 'query result is stored in variable \033[1m\033[97mdf\033[0m'
    try:
        from IPython.terminal.embed import InteractiveShellEmbed

        ipshell = InteractiveShellEmbed(colors='Linux')  # type: ignore
        ipshell(header=header, local_ns={'df': df})
    except ImportError:
        import code
        import sys

        class ExitInteract:
            def __call__(self) -> None:
                raise SystemExit

            def __repr__(self) -> str:
                raise SystemExit

        try:
            sys.ps1 = '>>> '
            code.interact(
                banner='\n' + header + '\n',
                local={'df': df, 'exit': ExitInteract()},
            )
        except SystemExit:
            pass


def determine_output_format(
    csv: bool, json: bool, ndjson: bool, output_file: str | None
) -> str:
    if csv + json + ndjson >= 2:
        raise Exception('can only specify one file format')
    elif csv or (output_file is not None and output_file.endswith('.csv')):
        return 'csv'
    elif json or (output_file is not None and output_file.endswith('.json')):
        return 'json'
    elif ndjson or (
        output_file is not None and output_file.endswith('.ndjson')
    ):
        return 'ndjson'
    else:
        return 'parquet'


def get_output_path(
    *,
    query: str,
    execution: spice._types.Execution,
    parameters: Mapping[str, Any] | None,
    api_key: str | None,
    performance: Literal['medium', 'large'],
    output_path: str | None,
    output_dir: str | None,
    format: str,
    query_name: str | None,
    label: str,
) -> str:
    import datetime

    if label is not None:
        output_template = (
            'dune__{query_name}__{label}__{execute_id}__{execute_time}.{format}'
        )
    else:
        output_template = (
            'dune__{query_name}__{execute_id}__{execute_time}.{format}'
        )

    query_id, _, parameters = spice._extract._determine_input_type(
        query, parameters
    )

    # get query name
    if query_name is not None:
        pass
    elif spice._extract._is_sql(query):
        query_name = 'RAW_SQL'
    else:
        query_name = str(query_id)

    # get time
    timestamp_secs = execution['timestamp']
    if timestamp_secs is None:
        raise Exception('could not obtain timestamp of execution')
    timestamp = datetime.datetime.fromtimestamp(float(timestamp_secs)).strftime(
        '%Y-%m-%d--%H-%M-%S'
    )

    return output_template.format(
        query_name=query_name,
        label=label,
        execute_id=execution['execution_id'],
        execute_time=timestamp,
        format=format,
    )


def save_file(df: pl.DataFrame, output_path: str, format: str) -> None:
    tmp_path = output_path + '_tmp'

    if format == 'parquet':
        df.write_parquet(tmp_path)
    elif format == 'csv':
        df.write_csv(tmp_path)
    elif format == 'json':
        df.write_json(tmp_path)
    elif format == 'ndjson':
        df.write_ndjson(tmp_path)
    else:
        raise Exception('invalid file format')

    import shutil

    shutil.move(tmp_path, output_path)


if __name__ == '__main__':
    run_cli()
