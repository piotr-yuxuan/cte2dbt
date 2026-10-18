"""The files written for a project: one per model, and sources.yml with quoted scalars."""

from cte2dbt import Model, Project, Source, dbt_files


def test_each_model_is_a_file_and_sources_are_grouped_by_schema() -> None:
    sales = Source("", "dbo", "Sales")
    on = Source("", "dbo", "On")
    archive = Source("Archive", "dbo", "Orders")
    awkward = Source("", "x: y", 'O\'Brien "x\\y"')
    project = Project(
        models=(Model("v", 'SELECT 1 FROM {{ source("dbo", "Sales") }} AS Sales', (), (sales,)),),
        sources=(sales, archive, awkward, on),
        warnings=(),
    )
    assert dbt_files(project) == {
        "v.sql": 'SELECT 1 FROM {{ source("dbo", "Sales") }} AS Sales\n',
        "sources.yml": (
            "version: 2\n"
            "\n"
            "sources:\n"
            '  - name: "dbo"\n'
            '    schema: "dbo"\n'
            "    tables:\n"
            '      - name: "On"\n'
            '      - name: "Sales"\n'
            '  - name: "x: y"\n'
            '    schema: "x: y"\n'
            "    tables:\n"
            '      - name: "O\'Brien \\"x\\\\y\\""\n'
            '  - name: "Archive__dbo"\n'
            '    database: "Archive"\n'
            '    schema: "dbo"\n'
            "    tables:\n"
            '      - name: "Orders"\n'
        ),
    }


def test_a_project_without_sources_has_an_empty_list() -> None:
    project = Project(models=(Model("v", "SELECT 1", (), ()),), sources=(), warnings=())
    assert dbt_files(project)["sources.yml"] == "version: 2\n\nsources: []\n"
