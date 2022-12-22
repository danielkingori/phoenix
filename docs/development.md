# Development

## Setup your development env
It is recommended that you use some sort of virtualisation to setup your development environment.
This repo has two options that are already set up: docker and virtualenv.

## Python Version
The current python version of this project is 3.9.2. This is the latest major version released minus 0.1.0 as of March 30 2021.

It is recommend that you manage you manage your system python installations with pyenv ([installation info](https://github.com/pyenv/pyenv#installation)).

Use `pyenv install` to install correct version.

If the version is updated then the files `.python-version` and Dockerfiles should also be updated.

## Virtualenv
It is recommended you manage your python package installs (i.e. per project) using python's built in venv.

Before installing dependencies, please make sure that your Python version matches what is contained in the `.python_version` file.

If the versions do not match, please refer to the above.

The terminal commands are then:
```
python -m venv .venv
source .venv/bin/activate
make install_all
```
Also recommended to install `pip-tools` into your virturalenv: `pip install pip-tools`

### Report
To run the report when in venv: `python phoenix/report/run.py`.
This will start a development server at [https://localhost:8050](https://localhost:8050)

## Docker
It is also possible to use docker for development. You will need to install `docker` and `docker-compose`. [Info](https://docs.docker.com/compose/install/).
Docker is useful because the environment that is run in production is the same as the one that you develop on.
This way you can use docker to debug and develop with the knowledge that what runs in development also runs in production.
```
./docker/dev up
```

You can then start a shell in the docker:
```
./docker/dev exec phoenix-dev bash
```
The code in docker will be automatically in sync with the code on your local machine.

The docker will also start a JupyterLab server that you can visit: [http://localhost:8888/lab](http://localhost:8888/lab)
So you can test and run code as you need.

### Report
To run the report using docker: `./docker/report up`.
This will start a development server at [https://localhost:8050](https://localhost:8050)

## CI/CD development
The current CI uses gitlab. You are able to test this locally.

Install `gitlab-runner` locally: https://docs.gitlab.com/runner/install/

You can then run the build locally:
`gitlab-runner exec shell build_test_all_image`


## Validation
The project also contains a validation suite. This suite is used to validate the algorithms.

Currently this is a simple test that will run the tagging on a list of sample messages and write the result as a CSV. The idea is that if the tagging is changed a PR can be opened with the changed output. Reviews of the PR can then see the results of the changes on the sample messages. In this way it is easy to assess and see the quality of the tagging functionality.

Changes to the tagging should then go like this:
- Change tagging code adding to `validation/tag/test_sample_messages.py` as needed
- run `make validate`
- check the output file `validation/tag/output_tagging.csv`
- commit the changed `validation/tag/output_tagging.csv` if correct
- make PR with the changes
