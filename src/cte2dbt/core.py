"""Split SQL view definitions into dbt models, one per common table expression."""

import graphlib
import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.dialects import TSQL, Oracle, Snowflake, SQLite
from sqlglot.dialects.dialect import Dialect
from sqlglot.errors import ErrorLevel, ParseError

_GO = re.compile(
    r"^[ \t]*GO(?:[ \t]+\d+)?[ \t]*;?[ \t]*(?:--[^\r\n]*)?\r?$", re.IGNORECASE | re.MULTILINE
)
"""A line that holds only the batch separator GO, with an optional count and comment."""

_VIEW = re.compile(r"\bVIEW\b", re.IGNORECASE)
"""The word in a statement that sqlglot kept as text which may define a view."""

_RECURSIVE_WITHOUT_KEYWORD = (TSQL, Oracle, Snowflake, SQLite)
"""Dialects in which a common table expression that names itself is recursive."""

_PLACEHOLDER = re.compile(r"__cte2dbt_(\d+)__")
"""Stands for the i-th reference of a model until its Jinja is written in."""

_JINJA_OPENING = re.compile(r"\{(?=[{%#])")
"""A brace that Jinja would read as the start of an expression, statement or comment."""

_NOT_IN_NAME = re.compile(r"\W+")
"""Characters replaced by an underscore in model names."""


@dataclass(frozen=True)
class View:
    """A view to convert: its name, its query and the column names it declares."""

    name: exp.Table
    query: exp.Query
    columns: tuple[exp.Identifier, ...] = ()


@dataclass(frozen=True)
class Script:
    """The views a script defines, in script order, and the statements it skipped."""

    views: tuple[View, ...]
    warnings: tuple[str, ...]


@dataclass(frozen=True, order=True)
class Source:
    """A table the views read but do not define: one table of a dbt source."""

    database: str
    schema: str
    table: str

    @property
    def source_name(self) -> str:
        """Return the dbt source name: the schema, after its database when there is one."""
        return f"{self.database}__{self.schema}" if self.database else self.schema


@dataclass(frozen=True)
class Model:
    """A dbt model: the query of a view, or one common table expression of it."""

    name: str
    sql: str
    refs: tuple[str, ...]
    sources: tuple[Source, ...]


@dataclass(frozen=True)
class Project:
    """Models after the models they ref, the sources they read, and what to review."""

    models: tuple[Model, ...]
    sources: tuple[Source, ...]
    warnings: tuple[str, ...]


@dataclass
class _Conversion:
    dialect: Dialect
    default_schema: str
    views: dict[tuple[str, ...], str] = field(default_factory=dict)
    spellings: dict[tuple[str, ...], str] = field(default_factory=dict)
    models: dict[str, Model] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass
class _Links:
    targets: list[str] = field(default_factory=list)
    refs: set[str] = field(default_factory=set)
    sources: set[Source] = field(default_factory=set)
    recursive: bool = False


def read_script(text: str, *, dialect: str) -> Script:
    """Return the CREATE VIEW statements of a script, as SSMS or SqlPackage export them.

    In a T-SQL script each line holding only GO becomes a semicolon, so that error line
    numbers stay those of the text. USE statements and SET options switched ON are
    ignored; any other statement is skipped with a warning. Raise ValueError for an
    unknown dialect, and sqlglot.errors.ParseError for SQL that sqlglot cannot parse, for
    SET QUOTED_IDENTIFIER OFF, and for a statement that may define a view but that sqlglot
    could only keep as text.
    """
    target = Dialect.get_or_raise(dialect)
    views: list[View] = []
    warnings: list[str] = []
    for statement in sqlglot.parse(
        _GO.sub(";", text) if isinstance(target, TSQL) else text, read=target
    ):
        if statement is None or isinstance(statement, exp.Use):
            continue
        sql = statement.sql(dialect=target)
        settings = (
            [item.sql(dialect=target).upper() for item in statement.expressions]
            if isinstance(statement, exp.Set)
            else []
        )
        if "QUOTED_IDENTIFIER OFF" in settings:
            msg = "SET QUOTED_IDENTIFIER OFF: double-quoted text after it would be read as names"
            raise ParseError(msg)
        if isinstance(statement, exp.Command) and _VIEW.search(sql):
            msg = f"sqlglot can only keep this statement as text: {sql[:80]}"
            raise ParseError(msg)
        if settings and all(setting.endswith(" ON") for setting in settings):
            continue
        if (
            isinstance(statement, exp.Create)
            and statement.kind == "VIEW"
            and isinstance(statement.expression, exp.Query)
        ):
            name = statement.this
            schema = name if isinstance(name, exp.Schema) else exp.Schema(this=name)
            columns = tuple(
                column.this if isinstance(column, exp.ColumnDef) else column
                for column in schema.expressions
            )
            views.append(View(schema.this, statement.expression, columns))
            continue
        warnings.append(f"skipped statement: {sql[:80]}")
    return Script(tuple(views), tuple(warnings))


def convert(views: Iterable[View], *, dialect: str, default_schema: str) -> Project:
    """Convert views into dbt models that ref() one another and read dbt sources.

    Each common table expression of a view becomes a model named <view>__<cte>, and the
    query of the view a model named after the view. A recursive common table expression
    keeps its WITH in its own model, and a column list wraps the query in a derived table
    that names the columns. A table that another view defines
    becomes a ref() of that view's model; any other stored table becomes a source(), in
    default_schema when it is named without a schema. Names compare as the dialect
    compares identifiers. The input views are not modified.

    Raise ValueError for an unknown dialect, when two models would share a name ignoring
    case, when views refer to one another in a cycle, when two sources would share a dbt
    source name, when dropping a schema from a column would bind it to another table, or
    when the SQL contains the reserved text __cte2dbt_.
    """
    conversion = _Conversion(Dialect.get_or_raise(dialect), default_schema)
    listed = tuple(views)
    for view in listed:
        conversion.views[_relation(view.name, conversion)[0]] = _model_name(view.name.name)
    implicit = isinstance(conversion.dialect, _RECURSIVE_WITHOUT_KEYWORD)
    for view in listed:
        prefix = _model_name(view.name.name)
        if prefix != view.name.name:
            conversion.warnings.append(f"{prefix}: renamed from {view.name.name}")
        query = view.query.copy()
        ctes = query.ctes
        clause = ctes[0].parent if ctes else None
        if isinstance(clause, exp.With):
            clause.pop()
        explicit = isinstance(clause, exp.With) and clause.recursive
        visible: dict[str, str] = {}
        for cte in ctes:
            alias: exp.TableAlias = cte.args["alias"]
            own = _key(alias.this, conversion.dialect)
            name = f"{prefix}__{_model_name(alias.name)}"
            links = _rewrite(
                cte.this, visible, own if implicit or explicit else None, conversion, name
            )
            body = (
                exp.select("*")
                .from_(exp.Table(this=alias.this.copy()))
                .with_(alias, as_=cte.this, recursive=explicit)
                if links.recursive
                else _wrap(cte.this, alias)
                if alias.columns
                else cte.this
            )
            _add(conversion, name, body, links)
            visible[own] = name
        links = _rewrite(query, visible, None, conversion, prefix)
        columns = [column.copy() for column in view.columns]
        alias = exp.TableAlias(this=view.name.this.copy(), columns=columns)
        _add(conversion, prefix, _wrap(query, alias) if columns else query, links)
    models = {model.name: model for model in conversion.models.values()}
    sorter = graphlib.TopologicalSorter({name: model.refs for name, model in models.items()})
    try:
        order = tuple(sorter.static_order())
    except graphlib.CycleError as error:
        msg = "views refer to one another in a cycle: " + " -> ".join(error.args[1])
        raise ValueError(msg) from error
    sources = sorted({source for model in models.values() for source in model.sources})
    names: dict[str, Source] = {}
    for source in sources:
        same = names.setdefault(source.source_name, source)
        if (same.database, same.schema) != (source.database, source.schema):
            msg = f"two dbt sources would be named {source.source_name}"
            raise ValueError(msg)
    return Project(
        tuple(models[name] for name in order), tuple(sources), tuple(conversion.warnings)
    )


def _rewrite(
    body: exp.Query,
    visible: Mapping[str, str],
    own: str | None,
    conversion: _Conversion,
    model: str,
) -> _Links:
    # Classify each table once, then point it at a placeholder for its ref() or source().
    dialect = conversion.dialect
    links = _Links()
    aliased: set[str] = set()
    for table in list(body.find_all(exp.Table)):
        name = table.this
        if (
            not isinstance(name, exp.Identifier)
            or name.args.get("temporary")
            or name.args.get("global_")
            or isinstance(table.parent, exp.Into)
        ):
            kept = table.sql(dialect=dialect, normalize_functions=False)
            conversion.warnings.append(f"{model}: {kept} kept as written")
            continue
        key, written = _relation(table, conversion)
        single = len(table.parts) == 1
        scope = table.parent
        while scope is not None and not (
            isinstance(scope, exp.Query)
            and key[-1] in {_key(cte.args["alias"].this, dialect) for cte in scope.ctes}
        ):
            scope = scope.parent
        if single and (scope is not None or key[-1] == own):
            links.recursive = links.recursive or scope is None
            continue
        target = (visible.get(key[-1]) if single else None) or conversion.views.get(key)
        if target:
            links.refs.add(target)
            links.targets.append(f"{{{{ ref({_literal(target)}) }}}}")
        else:
            source = Source(
                *(
                    conversion.spellings.setdefault(key[: i + 1], part)
                    for i, part in enumerate(written)
                )
            )
            links.sources.add(source)
            links.targets.append(
                f"{{{{ source({_literal(source.source_name)}, {_literal(source.table)}) }}}}"
            )
        if not table.alias:
            table.set("alias", exp.TableAlias(this=name.copy()))
            aliased.add(key[-1])
        table.set("this", exp.to_identifier(f"__cte2dbt_{len(links.targets) - 1}__"))
        table.set("db", None)
        table.set("catalog", None)
    exposed = Counter(
        _key(alias.this, dialect)
        for alias in body.find_all(exp.TableAlias)
        if isinstance(alias.this, exp.Identifier)
    )
    for column in body.find_all(exp.Column):
        qualifier = _key(column.args["table"], dialect) if column.args.get("db") else None
        if qualifier in aliased and exposed[qualifier] > 1:
            msg = (
                f"{model}: {column.sql(dialect=dialect)} would bind to another table once its"
                " schema is dropped; give the tables aliases"
            )
            raise ValueError(msg)
        if qualifier in aliased:
            column.set("db", None)
            column.set("catalog", None)
    return links


def _add(conversion: _Conversion, name: str, body: exp.Query, links: _Links) -> None:
    if name.casefold() in conversion.models:
        msg = f"two models would be named {name}, ignoring case"
        raise ValueError(msg)
    sql = body.sql(
        dialect=conversion.dialect,
        pretty=True,
        normalize_functions=False,
        unsupported_level=ErrorLevel.RAISE,
    )
    if len(_PLACEHOLDER.findall(sql)) != len(links.targets):
        msg = f"{name}: the SQL contains the reserved text __cte2dbt_"
        raise ValueError(msg)
    sql = _PLACEHOLDER.sub(
        lambda match: links.targets[int(match[1])], _JINJA_OPENING.sub("{{ '{' }}", sql)
    )
    conversion.models[name.casefold()] = Model(
        name, sql, tuple(sorted(links.refs)), tuple(sorted(links.sources))
    )


def _relation(table: exp.Table, conversion: _Conversion) -> tuple[tuple[str, ...], tuple[str, ...]]:
    # (database, schema, name) as compared and as written; T-SQL's db..name has an empty schema.
    parts = (
        table.args.get("catalog") or exp.to_identifier(""),
        table.args.get("db") or exp.to_identifier(conversion.default_schema),
        table.this,
    )
    return tuple(_key(part, conversion.dialect) for part in parts), tuple(p.name for p in parts)


def _key(identifier: exp.Expr, dialect: Dialect) -> str:
    # normalize_identifier edits its argument, hence the copy.
    return dialect.normalize_identifier(identifier.copy()).name


def _model_name(text: str) -> str:
    return _NOT_IN_NAME.sub("_", text)


def _wrap(query: exp.Query, alias: exp.TableAlias) -> exp.Select:
    return exp.select("*").from_(exp.Subquery(this=query, alias=alias))


def _literal(text: str) -> str:
    # A JSON string is both a Jinja string literal and a YAML double-quoted scalar.
    return json.dumps(text, ensure_ascii=False)
