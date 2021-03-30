# Project Structure
This project is a simple mono repo. This means that inside of `phoenix` there are seperate projects.

Each project:
- is independent of other projects
- has it's own project folder `phoenix/<project-name>`
- has it's own `requirements/<project-name>.in` and `requirements/<project-name>.txt`

The mono repo also has a common package that is used to shared code and dependencies across projects.

## Add a new project

Let [PROJECT] be the new project's module name. E.g. `fb-scraping`

1. Create dir `phoenix/[PROJECT]`
  * This is where python code specific to this project should go.
2. Create files `tests/unit/[PROJECT]/__init__.py` and `tests/integration/[PROJECT]/__init__.py`
  * These directories are where the unit and integration tests specific to this project should go.
3. Create file `requirements/[PROJECT].in`.
  * This file must start with the line `-r common.in` as first line, and should then contain project specific decencies. See `requirements/example.in` for an example.
  * Run `pip-compile requirements/[PROJECT].in` to compile the `in` file to the full `requirements/[PROJECT].txt` which pins all deps and sub-deps down to specific versions, to use in production.
4. Append `-r [PROJECT].in` to the end of the file `requirements/all.in`, and re-compile `all.in`, using `pip-compile requirements/all.in` .
  * `requirements/all.in` specifies deps that will be installed on development "all" env.

