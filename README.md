# Amstrax

[![Coverage Status](https://coveralls.io/repos/github/XAMS-nikhef/amstrax/badge.svg?branch=master)](https://coveralls.io/github/XAMS-nikhef/amstrax?branch=master)
[![CodeFactor](https://www.codefactor.io/repository/github/xams-nikhef/amstrax/badge)](https://www.codefactor.io/repository/github/xams-nikhef/amstrax)
[![PyPI version shields.io](https://img.shields.io/pypi/v/amstrax.svg)](https://pypi.python.org/pypi/amstrax/)
[![PyPI downloads](https://img.shields.io/pypi/dm/amstrax.svg)](https://pypistats.org/packages/amstrax)
[![DOI](https://zenodo.org/badge/263576054.svg)](https://zenodo.org/badge/latestdoi/263576054)
[![Python Versions](https://img.shields.io/pypi/pyversions/amstrax.svg)](https://pypi.python.org/pypi/amstrax)


## Documentation

[![Documentation Status](https://readthedocs.org/projects/amstrax/badge/?version=latest)](https://amstrax.readthedocs.io/en/latest/?badge=latest)

Amsterdam Strax

Amstrax is the analysis framework for XAMS, built on top of the
generic [strax framework](https://github.com/AxFoundation/strax).

# Installation

For installing the package do the regular install-packages-from-github tricks:

```
pip install amstrax
```

All required dependencies will be installed for you.

# Usage

First you'll need to get some data. For this you'll need to either run somewhere where nikhef's
/data/xenon/xams is available or have data downloaded. Either strax processed data or pax raw data
is fine. Once you have data you can try running the Tutorial notebook (although it's in Dutch), just
make sure to change the output_folder to where you have data
