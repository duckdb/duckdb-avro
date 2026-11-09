#!/usr/bin/env python3
"""A sqllogictest runner on the DuckDB Python package, for test/sql of this repository.

    run_sqllogic.py [--init-sql SQL] [--config key=value ...] [--verbose] <files>

Follows DuckDB's C++ runner (test/sqlite/sqllogic_test_runner.cpp, result_helper.cpp) for everything the tests here
use and the directives around them:

  statement ok | error | maybe [connection]      the expected error after `----` is a substring of the error text
                                                 (or a `<REGEX>:` / `<!REGEX>:` full match over the text + "\\n")
  query <types> [nosort|rowsort|valuesort] [label]
                                                 values rendered as DuckDB casts them to VARCHAR (NULL -> `NULL`,
                                                 `` -> `(empty)`, booleans `true`/`false`), the result sorted per the
                                                 modifier (default nosort), compared row-wise (tab-separated) or one
                                                 value per line, cell by cell: string equality, else `<REGEX>:`, else
                                                 numeric columns compared as numbers (FLOAT/DOUBLE approximately),
                                                 booleans leniently (`true`/`1`); `N values hashing to <md5>` compares
                                                 the digest of the rendered values; `<FILE>:path` reads them from a file
  require <feature>                              platform / build requires evaluated here; `avro` loads the file
                                                 named by $AVRO_EXTENSION on every connection (skips the file when it
                                                 is unset); any other name is `LOAD`ed (bundled icu / json / parquet)
                                                 and skips the file when that fails
  require-env VAR [value]                        skips the file when VAR is unset (or differs); defines {VAR}
  loop / foreach / endloop                       nested, with {var} (and deprecated ${var}) substitution in the SQL
                                                 and the expected values; the `<signed>` ... type groups of foreach;
                                                 concurrentloop / concurrentforeach run sequentially
  skipif / onlyif <system | condition>           `duckdb` system comparisons and `i=2`, `i<>2 && j>0` conditions on
                                                 loop variables
  mode skip|unskip|output_hash|output_result|debug|no_output, set ignore_error_messages|always_fail_error_messages
  |seed|variable, reset, hash-threshold, halt, sleep, unzip, load [path] [readonly], restart [no_extension_load],
  reconnect
  {TEST_DIR} / __TEST_DIR__ (a fresh temporary directory per file), __WORKING_DIRECTORY__, __BUILD_DIRECTORY__

The fixtures are named relative to the repository root: run with that as the working directory.
"""
import argparse
import gzip
import hashlib
import math
import os
import platform
import re
import shutil
import sys
import tempfile
import time

import duckdb

NUMERIC_IDS = {
    'tinyint', 'smallint', 'integer', 'bigint', 'hugeint', 'utinyint', 'usmallint', 'uinteger', 'ubigint', 'uhugeint',
    'float', 'double', 'decimal',
}
FLOAT_IDS = {'float', 'double'}
SIGNED = ['tinyint', 'smallint', 'integer', 'bigint', 'hugeint']
UNSIGNED = ['utinyint', 'usmallint', 'uinteger', 'ubigint', 'uhugeint']
ALL_TYPES = [
    'bool', 'interval', 'varchar', 'json', 'blob', 'bit', 'uuid', 'date', 'time', 'timestamp', 'timestamp_s',
    'timestamp_ms', 'timestamp_ns', 'timestamptz', 'timetz',
]
COMPRESSIONS = ['uncompressed', 'rle', 'bitpacking', 'dictionary', 'fsst', 'chimp', 'patas', 'alp', 'alprd', 'roaring']


class RunnerError(Exception):
    """A problem of the test file or the runner itself, not of the engine under test."""


class Skip(Exception):
    """The file is skipped: a requirement is missing."""


class Halt(Exception):
    """`halt`: stop processing the file."""


# -------------------------------------------------------------------------------------------------
# Parsing
# -------------------------------------------------------------------------------------------------


class Command:
    def __init__(self, kind, params, line, sql=None, expected=None, conditions=None):
        self.kind = kind  # the directive
        self.params = params  # the directive's parameters
        self.line = line  # 1-based line of the directive
        self.sql = sql  # the statement text (statement / query)
        self.expected = expected  # the lines after `----` (None when there was no `----`)
        self.conditions = conditions or []  # (skip_if: bool, text) of the skipif / onlyif lines before it
        self.body = []  # the commands of a loop


def parse(path):
    lines = open(path, encoding='utf-8', errors='surrogateescape').read().split('\n')
    i = 0
    n = len(lines)
    root = []
    stack = [root]
    conditions = []
    while i < n:
        line = lines[i]
        stripped = line.strip()
        if stripped == '' or stripped.startswith('#'):
            i += 1
            continue
        parts = stripped.split()
        kind, params = parts[0], parts[1:]
        start = i + 1
        if kind in ('skipif', 'onlyif'):
            if not params:
                raise RunnerError(f'{path}:{start}: {kind} requires a parameter')
            conditions.append((kind == 'skipif', ' '.join(params)))
            i += 1
            continue
        if kind in ('statement', 'query'):
            i += 1
            sql = []
            while i < n and lines[i].strip() != '' and not lines[i].startswith('----'):
                sql.append(lines[i])
                i += 1
            expected = None
            if i < n and lines[i].startswith('----'):
                i += 1
                expected = []
                while i < n and lines[i] != '':
                    expected.append(lines[i])
                    i += 1
            if not sql:
                raise RunnerError(f'{path}:{start}: {kind} without SQL')
            cmd = Command(kind, params, start, '\n'.join(sql), expected, conditions)
            conditions = []
            stack[-1].append(cmd)
            continue
        i += 1
        cmd = Command(kind, params, start, conditions=conditions)
        conditions = []
        if kind in ('loop', 'foreach', 'concurrentloop', 'concurrentforeach'):
            stack[-1].append(cmd)
            stack.append(cmd.body)
            continue
        if kind == 'endloop':
            if len(stack) == 1:
                raise RunnerError(f'{path}:{start}: endloop without loop')
            stack.pop()
            continue
        stack[-1].append(cmd)
    if len(stack) != 1:
        raise RunnerError(f'{path}: loop without endloop')
    return root


# -------------------------------------------------------------------------------------------------
# Execution
# -------------------------------------------------------------------------------------------------


def approx_equal(l, r):
    if math.isnan(l) and math.isnan(r):
        return True
    if math.isinf(l) or math.isinf(r):
        return l == r
    return abs(l - r) <= abs(r) * 0.01 + 0.00000001


def regex_matches(actual, expected):
    want = expected.startswith('<REGEX>:')
    pattern = expected.replace('<REGEX>:', '', 1) if want else expected.replace('<!REGEX>:', '', 1)
    try:
        matched = re.fullmatch(pattern, actual, re.S) is not None
    except re.error as e:
        raise RunnerError(f'failed to parse regex {pattern!r}: {e}') from None
    return matched if want else not matched


class FileRunner:
    def __init__(self, path, init_sql, config, verbose):
        self.path = path
        self.init_sql = init_sql
        self.config = dict(config)
        self.verbose = verbose
        self.test_dir = tempfile.mkdtemp(prefix='sqllogic_')
        self.db_path = ''  # in-memory
        self.read_only = False
        self.db = None
        self.connections = {}
        self.loaded = []  # SQL run on every new connection (LOADs of `require`)
        self.env_vars = {}  # require-env / set variable
        self.loops = []  # (name, value) of the active loops, innermost last
        self.skip_mode = False
        self.hash_threshold = 0
        self.ignore_errors = []
        self.always_fail_errors = []
        self.labels = {}
        self.checks = 0
        self.failures = []
        self.open_database()

    # --- database ----------------------------------------------------------------------------------

    def open_database(self):
        for c in self.connections.values():
            c.close()
        if self.db is not None:
            self.db.close()
        config = dict(self.config)
        if self.read_only:
            config['access_mode'] = 'READ_ONLY'
        self.db = duckdb.connect(self.db_path or ':memory:', config=config)
        self.connections = {}
        self.setup_connection(self.db)

    def setup_connection(self, con):
        if self.init_sql:
            con.execute(self.init_sql)
        for sql in self.loaded:
            con.execute(sql)

    def connection(self, label):
        if not label:
            return self.db
        if label not in self.connections:
            con = self.db.cursor()
            self.setup_connection(con)
            self.connections[label] = con
        return self.connections[label]

    def load_on_all(self, sql):
        self.loaded.append(sql)
        self.db.execute(sql)
        for c in self.connections.values():
            c.execute(sql)

    # --- substitution ------------------------------------------------------------------------------

    def replace_keywords(self, text):
        for name, value in self.env_vars.items():
            text = text.replace('${' + name + '}', value).replace('{' + name + '}', value)
        text = text.replace('__TEST_DIR__', self.test_dir).replace('{TEST_DIR}', self.test_dir)
        text = text.replace('${TEST_DIR}', self.test_dir)
        text = text.replace('__WORKING_DIRECTORY__', os.getcwd()).replace('{WORKING_DIRECTORY}', os.getcwd())
        build = os.path.join(os.getcwd(), 'build')
        text = text.replace('__BUILD_DIRECTORY__', build).replace('{BUILD_DIRECTORY}', build)
        return text

    def substitute(self, text):
        for name, value in self.loops:
            text = text.replace('${' + name + '}', value).replace('{' + name + '}', value)
        return self.replace_keywords(text)

    # --- conditions --------------------------------------------------------------------------------

    def condition_holds(self, skip_if, text):
        """True when the command runs: `skipif duckdb` skips, `onlyif duckdb` runs, conditions on loop variables."""
        if not any(c in text for c in '=<>'):
            ours = text.lower() == 'duckdb'
            return (not ours) if skip_if else ours
        result = True
        for part in text.split('&&'):
            part = part.strip()
            op = None
            for candidate in ('<>', '>=', '<=', '>', '<', '='):
                if candidate in part:
                    op = candidate
                    break
            name, value = (s.strip() for s in part.split(op, 1))
            actual = self.substitute('{' + name + '}')
            if actual == '{' + name + '}':
                raise RunnerError(f'{self.path}: condition on unknown loop variable {name!r}')
            try:
                a, b = float(actual), float(value)
            except ValueError:
                a, b = actual, value
            holds = {
                '=': a == b, '<>': a != b, '>': a > b, '>=': a >= b, '<': a < b, '<=': a <= b,
            }[op]
            result = result and holds
        return (not result) if skip_if else result

    def should_run(self, cmd):
        return all(self.condition_holds(skip_if, text) for skip_if, text in cmd.conditions)

    # --- commands ----------------------------------------------------------------------------------

    def run(self, commands):
        for cmd in commands:
            if cmd.kind in ('loop', 'foreach', 'concurrentloop', 'concurrentforeach'):
                self.run_loop(cmd)
                continue
            if not self.should_run(cmd):
                continue
            if self.skip_mode and cmd.kind not in ('mode',):
                continue
            handler = getattr(self, 'cmd_' + cmd.kind.replace('-', '_'), None)
            if handler is None:
                raise RunnerError(f'{self.path}:{cmd.line}: unknown directive {cmd.kind!r}')
            handler(cmd)

    def run_loop(self, cmd):
        if cmd.kind in ('loop', 'concurrentloop'):
            if len(cmd.params) != 3:
                raise RunnerError(f'{self.path}:{cmd.line}: loop requires: loop <name> <start> <end>')
            name = cmd.params[0]
            try:
                start, end = int(self.substitute(cmd.params[1])), int(self.substitute(cmd.params[2]))
            except ValueError:
                raise RunnerError(f'{self.path}:{cmd.line}: loop_start and loop_end must be a number') from None
            values = [str(v) for v in range(start, end)]
        else:
            if len(cmd.params) < 2:
                raise RunnerError(f'{self.path}:{cmd.line}: foreach requires: foreach <name> <values...>')
            name = cmd.params[0]
            values = self.foreach_values(cmd.params[1:])
        if not self.should_run(cmd):
            return
        for value in values:
            value = self.replace_keywords(value)
            if ',' in name:
                names, parts = name.split(','), value.split(',')
                if len(names) != len(parts):
                    raise RunnerError(f'{self.path}:{cmd.line}: foreach: {name} does not match {value}')
                pushed = list(zip(names, parts))
            else:
                pushed = [(name, value)]
            self.loops.extend(pushed)
            try:
                self.run(cmd.body)
            finally:
                del self.loops[len(self.loops) - len(pushed):]

    def foreach_values(self, params):
        result = []
        for param in params:
            token = param.lower()
            if token.startswith('!'):
                if param[1:] in result:
                    result.remove(param[1:])
                else:
                    result.append(param)
                continue
            is_all = token == '<alltypes>'
            is_numeric = is_all or token == '<numeric>'
            is_integral = is_numeric or token == '<integral>'
            group = False
            if is_integral or token == '<signed>':
                result += SIGNED
                group = True
            if is_integral or token == '<unsigned>':
                result += UNSIGNED
                group = True
            if is_numeric:
                result += ['float', 'double']
                group = True
            if is_all:
                result += ALL_TYPES
                group = True
            if token == '<compression>':
                result += COMPRESSIONS
                group = True
            if not group:
                result.append(param)
        return result

    def cmd_halt(self, cmd):
        raise Halt()

    def cmd_mode(self, cmd):
        mode = cmd.params[0] if cmd.params else ''
        if mode == 'skip':
            self.skip_mode = True
        elif mode == 'unskip':
            self.skip_mode = False
        elif mode in ('output_hash', 'output_result', 'debug', 'no_output'):
            pass
        else:
            raise RunnerError(f'{self.path}:{cmd.line}: unrecognized mode {mode!r}')

    def cmd_hash_threshold(self, cmd):
        self.hash_threshold = int(cmd.params[0])

    def cmd_set(self, cmd):
        if not cmd.params:
            raise RunnerError(f'{self.path}:{cmd.line}: set requires a parameter')
        what = cmd.params[0]
        if what in ('ignore_error_messages', 'always_fail_error_messages'):
            messages = [m.strip() for m in ' '.join(cmd.params[1:]).split(',') if m.strip()]
            (self.ignore_errors if what == 'ignore_error_messages' else self.always_fail_errors).extend(messages)
        elif what == 'seed':
            self.db.execute(f'SELECT setseed({cmd.params[1]})')
        elif what == 'variable':
            if len(cmd.params) < 3:
                raise RunnerError(f'{self.path}:{cmd.line}: set variable <name> <value>')
            self.env_vars[cmd.params[1]] = self.substitute(' '.join(cmd.params[2:]))
        else:
            raise RunnerError(f'{self.path}:{cmd.line}: unrecognized set parameter {what!r}')

    def cmd_reset(self, cmd):
        if len(cmd.params) == 2 and cmd.params[0] == 'label':
            self.labels.pop(cmd.params[1], None)
        elif len(cmd.params) == 2 and cmd.params[0] == 'variable':
            self.env_vars.pop(cmd.params[1], None)
        else:
            raise RunnerError(f'{self.path}:{cmd.line}: reset label <name> | reset variable <name>')

    def cmd_sleep(self, cmd):
        if len(cmd.params) != 2:
            raise RunnerError(f'{self.path}:{cmd.line}: sleep <count> <unit>')
        count = float(cmd.params[0])
        unit = cmd.params[1].lower().rstrip('s')
        scale = {'second': 1, 'millisecond': 1e-3, 'microsecond': 1e-6, 'nanosecond': 1e-9}.get(unit)
        if scale is None:
            raise RunnerError(f'{self.path}:{cmd.line}: unrecognized sleep unit {cmd.params[1]!r}')
        time.sleep(count * scale)

    def cmd_unzip(self, cmd):
        if len(cmd.params) not in (1, 2):
            raise RunnerError(f'{self.path}:{cmd.line}: unzip <source.gz> [destination]')
        source = self.substitute(cmd.params[0])
        destination = self.substitute(cmd.params[1]) if len(cmd.params) == 2 else os.path.join(
            self.test_dir, os.path.basename(source).removesuffix('.gz'))
        with gzip.open(source, 'rb') as src, open(destination, 'wb') as dst:
            shutil.copyfileobj(src, dst)

    def cmd_load(self, cmd):
        self.db_path = self.substitute(cmd.params[0]) if cmd.params else ''
        self.read_only = len(cmd.params) > 1 and cmd.params[1] == 'readonly'
        self.open_database()

    def cmd_restart(self, cmd):
        if not self.db_path:
            raise RunnerError(f'{self.path}:{cmd.line}: restart requires a database loaded with `load`')
        keep = self.loaded
        if cmd.params and cmd.params[0] == 'no_extension_load':
            self.loaded = []
        self.open_database()
        self.loaded = keep

    def cmd_reconnect(self, cmd):
        self.open_database()

    def cmd_require_env(self, cmd):
        if not cmd.params:
            raise RunnerError(f'{self.path}:{cmd.line}: require-env <VAR> [value]')
        name = cmd.params[0]
        actual = os.environ.get(name)
        if actual is None:
            raise Skip(f'require-env {name}')
        if len(cmd.params) > 1 and actual != cmd.params[1]:
            raise Skip(f'require-env {name}={cmd.params[1]}')
        self.env_vars[name] = actual

    def cmd_require(self, cmd):
        if not cmd.params:
            raise RunnerError(f'{self.path}:{cmd.line}: require <feature>')
        param = cmd.params[0].lower()
        windows = sys.platform.startswith('win')
        linux = sys.platform.startswith('linux')
        present = {
            'notmusl': True, 'notmingw': True, 'mingw': False, 'notwindows': not windows, 'windows': windows,
            'linux': linux, 'longdouble': True, '64bit': platform.architecture()[0] == '64bit',
            'noforcestorage': True, 'no_force_storage': True, 'nothreadsan': True, 'strinline': True,
            'skip_reload': True, 'no_alternative_verify': True, 'no_latest_storage': True,
            'no_vector_verification': True, 'no_extension_autoloading': True, 'allow_unsigned_extensions': True,
            'vacuum_rebuild_indexes': True,
        }
        if param in present:
            if not present[param]:
                raise Skip(f'require {param}')
            return
        if param in ('ram', 'disk_space'):
            return
        if param in ('vector_size', 'exact_vector_size', 'block_size'):
            if len(cmd.params) < 2:
                raise RunnerError(f'{self.path}:{cmd.line}: require {param} <N>')
            wanted = int(cmd.params[1])
            if param == 'vector_size' and wanted > 2048:
                raise Skip(f'require vector_size {wanted}')
            if param == 'exact_vector_size' and wanted != 2048:
                raise Skip(f'require exact_vector_size {wanted}')
            if param == 'block_size' and wanted != 262144:
                raise Skip(f'require block_size {wanted}')
            return
        # an extension
        if param == 'avro':
            path = os.environ.get('AVRO_EXTENSION')
            if not path:
                raise Skip('require avro (AVRO_EXTENSION unset)')
            sql = "LOAD '" + path.replace("'", "''") + "'"
        else:
            sql = f'LOAD {param}'
        try:
            self.load_on_all(sql)
        except duckdb.Error as e:
            raise Skip(f'require {param}: {str(e).splitlines()[0]}') from None

    # --- statements and queries --------------------------------------------------------------------

    def fail(self, cmd, message):
        self.failures.append(f'{self.path}:{cmd.line}: {message}')

    def check_skip_error(self, message):
        """`set ignore_error_messages`: an unexpected error naming one of them skips the rest of the file."""
        if any(m in message for m in self.always_fail_errors):
            return False
        if any(m in message for m in self.ignore_errors):
            raise Skip(f'ignored error: {message.splitlines()[0]}')
        return False

    def error_matches(self, expected, actual):
        if expected in actual or self.replace_keywords(expected) in actual:
            return True
        if expected.startswith('<REGEX>:') or expected.startswith('<!REGEX>:'):
            return regex_matches(actual + '\n', expected)
        # The Python client renders an exception type it has no class for (InvalidConfigurationException, ...) as a
        # bare duckdb.Error carrying the message without its "<Type> Error: " prefix; DuckDB's own runner sees the
        # prefix. Match such an expectation without the prefix when the actual text starts without one.
        prefixed = re.match(r'^[A-Za-z ]+ Error: ', expected)
        if prefixed and not re.match(r'^[A-Za-z ]+ Error: ', actual):
            return self.error_matches(expected[prefixed.end():], actual)
        return False

    def cmd_statement(self, cmd):
        if not cmd.params or cmd.params[0] not in ('ok', 'error', 'maybe'):
            raise RunnerError(f'{self.path}:{cmd.line}: statement ok|error|maybe')
        expect = cmd.params[0]
        con = self.connection(cmd.params[1] if len(cmd.params) > 1 else '')
        sql = self.substitute(cmd.sql)
        expected_error = self.substitute('\n'.join(cmd.expected)) if cmd.expected else ''
        self.checks += 1
        if self.verbose:
            print(f'  {cmd.line}: statement {expect}')
        error = None
        try:
            con.execute(sql)
            try:
                con.fetchall()
            except duckdb.Error as e:
                error = str(e)
        except duckdb.Error as e:
            error = str(e)
        if expect == 'ok':
            if error is not None:
                self.check_skip_error(error)
                self.fail(cmd, f'query unexpectedly failed:\n{sql}\n  {error}')
            return
        if error is None:
            if expect == 'error':
                self.fail(cmd, f'query unexpectedly succeeded:\n{sql}')
            return
        if expected_error and not self.error_matches(expected_error, error):
            self.check_skip_error(error)
            self.fail(cmd, f'query failed, but the error did not match the expected {expected_error!r}:\n{sql}\n  {error}')

    def render(self, rel):
        """The values as DuckDB's runner renders them, one list per row."""
        types = rel.types
        rows = rel.project('CAST(COLUMNS(*) AS VARCHAR)').fetchall()
        out = []
        for row in rows:
            cells = []
            for value, type_ in zip(row, types):
                if value is None:
                    cells.append('NULL')
                elif type_.id == 'boolean':
                    cells.append('true' if value == 'true' else 'false')
                elif value == '':
                    cells.append('(empty)')
                else:
                    cells.append(value.replace('\0', '\\0'))
            out.append(cells)
        return out, types

    def compare_values(self, actual, expected, type_):
        if actual == expected or actual == self.replace_keywords(expected):
            return True
        if expected.startswith('<REGEX>:') or expected.startswith('<!REGEX>:'):
            if regex_matches(actual, expected):
                return True
        tid = type_.id
        if tid in NUMERIC_IDS:
            if actual == 'NULL' or expected == 'NULL':
                return False
            try:
                if tid in FLOAT_IDS:
                    return approx_equal(float(actual), float(expected))
                if tid == 'decimal':
                    from decimal import Decimal, InvalidOperation
                    try:
                        return Decimal(actual) == Decimal(expected)
                    except InvalidOperation:
                        return False
                return int(actual) == int(expected)
            except ValueError:
                return False
        if tid == 'boolean':
            truth = {'true': True, '1': True, 'false': False, '0': False}
            return truth.get(actual.lower()) is not None and truth.get(actual.lower()) == truth.get(expected.lower())
        return False

    def cmd_query(self, cmd):
        if not cmd.params:
            raise RunnerError(f'{self.path}:{cmd.line}: query <types> [sort] [label]')
        types_spec = cmd.params[0]
        sort_style = 'nosort'
        label = None
        for param in cmd.params[1:]:
            if param in ('nosort', 'rowsort', 'valuesort'):
                sort_style = param
            elif param.startswith('con'):
                pass  # a connection label is not used by these tests beyond the default
            else:
                label = param
        con = self.db
        sql = self.substitute(cmd.sql)
        expected = [self.substitute(line) for line in (cmd.expected or [])]
        self.checks += 1
        if self.verbose:
            print(f'  {cmd.line}: query {types_spec} {sort_style}')
        try:
            rel = con.sql(sql)
            if rel is None:
                raise RunnerError(f'{self.path}:{cmd.line}: the query returned no result:\n{sql}')
            rows, types = self.render(rel)
        except duckdb.Error as e:
            self.check_skip_error(str(e))
            self.fail(cmd, f'query unexpectedly failed:\n{sql}\n  {e}')
            return
        column_count = len(types)
        # sort
        if sort_style == 'rowsort':
            rows = sorted(rows)
        values = [v for row in rows for v in row]
        if sort_style == 'valuesort':
            values = sorted(values)
        total = len(values)
        # `<FILE>:` and hash results
        if len(expected) == 1 and expected[0].startswith('<FILE>:'):
            with open(self.replace_keywords(expected[0][len('<FILE>:'):]).strip(), encoding='utf-8') as f:
                expected = [line for line in f.read().split('\n')]
            while expected and expected[-1] == '':
                expected.pop()
        hash_digest = None
        if label is not None or (len(expected) == 1 and ' values hashing to ' in expected[0]):
            md5 = hashlib.md5()
            for v in values:
                # DuckDB hashes its own rendering, where a boolean is `1` / `0`
                md5.update((v if v not in ('true', 'false') else ('1' if v == 'true' else '0')).encode())
                md5.update(b'\n')
            hash_digest = f'{total} values hashing to {md5.hexdigest()}'
            if label is not None:
                if label in self.labels and self.labels[label] != hash_digest:
                    self.fail(cmd, f'result differs from the earlier result labelled {label}:\n{sql}')
                    return
                self.labels[label] = hash_digest
            if len(expected) == 1 and ' values hashing to ' in expected[0]:
                if expected[0] != hash_digest:
                    self.fail(cmd, f'hash mismatch: expected {expected[0]!r}, got {hash_digest!r}:\n{sql}')
                return
            if label is not None and not expected:
                return
        # the column count is checked for a comparison by values only, as DuckDB's runner does (a query compared by
        # label or hash passes whatever its type string says)
        expected_columns = len(types_spec)
        if expected_columns != column_count:
            self.fail(cmd, f'expected {expected_columns} columns, but got {column_count}:\n{sql}')
            return
        # row-wise (tab separated) or one value per line
        row_wise = column_count > 1 and len(expected) == len(rows)
        if not row_wise:
            row_wise = bool(expected) and all('\t' in line for line in expected)
        if row_wise:
            expected_rows = len(expected)
        else:
            if column_count > 0 and len(expected) % column_count != 0:
                self.fail(cmd, f'expected {len(expected)} values, not cleanly divisible by {column_count} columns:\n{sql}')
                return
            expected_rows = len(expected) // column_count if column_count else 0
        if expected_rows != len(rows):
            self.fail(cmd, f'expected {expected_rows} rows, but got {len(rows)}:\n{sql}\n  expected: {expected}\n  got:      {[chr(9).join(r) for r in rows]}')
            return
        if row_wise:
            flat = []
            for i, line in enumerate(expected):
                cells = line.split('\t')
                if len(cells) != column_count:
                    self.fail(cmd, f'expected {column_count} columns on row {i + 1}, but the line has {len(cells)}:\n{sql}')
                    return
                flat += cells
            expected = flat
        for i, (actual, want) in enumerate(zip(values, expected)):
            type_ = types[i % column_count]
            if not self.compare_values(actual, want, type_):
                row, col = i // column_count + 1, i % column_count + 1
                self.fail(cmd, f'wrong result on row {row}, column {col}: {actual!r} <> {want!r}:\n{sql}\n  expected: {expected}\n  got:      {values}')
                return

    # --- lifecycle ---------------------------------------------------------------------------------

    def close(self):
        for c in self.connections.values():
            c.close()
        if self.db is not None:
            self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)


def run_file(path, init_sql, config, verbose):
    commands = parse(path)
    runner = FileRunner(path, init_sql, config, verbose)
    try:
        try:
            runner.run(commands)
        except Halt:
            pass
        except Skip as skip:
            return 'skipped', str(skip), []
        return ('failed' if runner.failures else 'passed'), f'{runner.checks} checks, {len(runner.failures)} failures', runner.failures
    finally:
        runner.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    ap.add_argument('--init-sql', default=os.environ.get('AVRO_TEST_INIT_SQL', ''),
                    help='SQL run on every connection first')
    ap.add_argument('--config', action='append', default=[], help='DuckDB config option key=value')
    ap.add_argument('--verbose', '-v', action='store_true', help='print every statement as it runs')
    ap.add_argument('files', nargs='+')
    args = ap.parse_args()
    config = {'allow_unsigned_extensions': 'true'}
    for kv in args.config:
        k, v = kv.split('=', 1)
        config[k] = v
    totals = {'passed': 0, 'failed': 0, 'skipped': 0, 'error': 0}
    for path in args.files:
        try:
            status, detail, failures = run_file(path, args.init_sql, config, args.verbose)
        except RunnerError as e:
            status, detail, failures = 'error', f'runner error: {e}', []
        totals[status] += 1
        for failure in failures:
            print(failure)
        print(f'{status:8} {path} ({detail})', flush=True)
    print(f"\n{totals['passed']} passed, {totals['failed']} failed, {totals['skipped']} skipped, {totals['error']} runner errors")
    sys.exit(0 if totals['failed'] == 0 and totals['error'] == 0 else 1)


if __name__ == '__main__':
    main()
