#!/bin/bash
rm -rf dist/
python -m build
pip install -e . 