# `cte2dbt`

This small Python module is here to take a large SQL file containing
an overwhelming query with a lot of common table expressions (CTE) and
split them in smaller, digestible dbt models.

The intent is to turn ad-hoc queries written on the spot by analysts
into testable (tested), robust data live pipelines.

## Technical architecture

We are using a Python library that provides a SQL parser, se we can
abstract away the text and reason on tokens. This parser exposes
low-level tokens, for example whitespace characters or keywords. Some
tokens may be grouped together for additional se,antic like an
identifier that would be just a name `order`, or a name with an alias
`order AS o`.

As stated above, the high-level goal of this library is to split a
query using CTE into dbt models. Slightly more precisely, this library
performs a reduction on CTE, builds a context of the names it has
previously seen a replaces them where appriopriate.

Going down one level of abstraction, it is about isolating for each
CTE its name and query text, replace the former appropriately
everywhere they occur and write the latter in a file.

Explained more precisely, we expect the parsed query to contain at
least one `WITH` construct that introduces a list of identifiers that
follow the syntax `name AS query`. This (long) construct is most
likely followed by a `SELECT` query that uses the CTE names defined
above. This library processes each CTE by calling function
`render_hook` with its tranformed name, transformed query text and a
couple of other arguments (called `context`) to enable extensibility.
The transformed name is the output of function `rendered_name` called
with the original name of the CTE alongside its que
ry text and the
same `context`.

The crux of this work happens around transforming the query texts: for
example, name `CTE1` might get replaced by dbt model `stg_cte_1` or
even a reference in the form of `{{ ref('stg_cte_1') }}`. We should
remember names once they are declared and replace them in all
subsequent query texts, including the own text of the CTE if it is
recursive.
