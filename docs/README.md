# Documentation

- **Tutorial:** [Split your first view](tutorials/split-your-first-view.md): two T-SQL
  views become dbt models, refs and sources, one step at a time.
- **How-to:** [convert Azure SQL views into a dbt project](how-to/convert-azure-sql-views.md);
  [run the models without dbt](how-to/run-models-without-dbt.md);
  [develop and integrate a change](how-to/develop.md).
- **Reference:** [command line](reference/command-line.md);
  [Python API](reference/python-api.md); [conversion rules](reference/conversion-rules.md);
  [checks](reference/checks.md); [continuous integration](reference/ci.md);
  [reuse and attribution](reference/reuse.md).
- **Explanation:** [how views become models](explanation/conversion.md);
  [architecture](explanation/architecture.md).
- **Work:** [roadmap](work/README.md).

These pages follow [Diátaxis](https://diataxis.fr/): the tutorial teaches by doing, a
how-to guide reaches a goal, reference describes facts, and explanation gives reasons. A
section exists only once its first page does. Work plans and their archive in
`docs/changelog/` are records of delivery, not current documentation.

For each change, update the affected page in the same change and run
`uv run --locked python dev/scripts/check.py docs`: it resolves local links and runs every
Python example. Check the meaning of each page against the changed behaviour; a passing
example can still sit beside a stale sentence.
