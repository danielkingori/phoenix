# Development

## Setup your development env
It is recommended that you use some sort of vitualisation to setup your development environment.
This repo has two options that are already set up: docker and virturalenv.

## Virtualenv
It is recommended that you manage you manage your system python installations with pyenv ([installation info](https://github.com/pyenv/pyenv#installation)).
And it is recommended you manage your python package installs (i.e. per project) using python's built in venv.

The terminal commands are then:
```
pyenv install
python -m venv venv
source venv/bin/activate
make install_all
```
Also recommended to install `pip-tools` into your virturalenv: `pip install pip-tools`

## Docker
It is also possible to use docker for development. You will need to install `docker` and `docker-compose`. [Info](https://docs.docker.com/compose/install/).
Docker is useful because the environment that is run in production is the same as the one that you develop on.
This way you can use docker to debug and develop with the knowledge that what runs in development also runs in production.
```
docker-compose up
```

You can then start a shell in the docker:
```
docker-compose exec phoenix-dev bash
```
The code in docker will be automatically in sync with the code on your local machine.

The docker will also start a JupyterLab server that you can visit: [http://localhost:8888/lab](http://localhost:8888/lab)
So you can test and run code as you need.
