# Getting Started

## Good to read
Before you start it is recommended that you read the documentation:
- [Project Structure and adding a new project](/docs/project-structure.md)

## Setup your development env
It is recommended that you use some sort of vitualisation to setup your development environment.
This repo has two options that are already set up: docker and virturalenv.

### Virtualenv
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
