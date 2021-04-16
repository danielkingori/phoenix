#!/bin/bash

# Install editable so that it updates on changes
pip install -e .

exec $@
