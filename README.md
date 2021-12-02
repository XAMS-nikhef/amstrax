# Amstrax
[![Coverage Status](https://coveralls.io/repos/github/XAMS-nikhef/amstrax/badge.svg?branch=master)](https://coveralls.io/github/XAMS-nikhef/amstrax?branch=master)
[![CodeFactor](https://www.codefactor.io/repository/github/xams-nikhef/amstrax/badge)](https://www.codefactor.io/repository/github/xams-nikhef/amstrax)

Amsterdam Strax

Amstrax is the analysis framework for XAMS, built on top of the generic [strax framework](https://github.com/AxFoundation/strax). 

# Installation
For installing the package do the regular instal-packages-from-github tricks:
```
git clone https://github.com/XAMS-nikhef/amstrax
python ./amstrax/setup.py develop
```
All required dependancies will be installed for you.

# Usage
First you'll need to get some data. For this you'll need to either run somewhere where nikhef's /data/xenon/xams is available or have data downloaded.
Either strax processed data or pax raw data is fine.
Once you have data you can try running the Tutorial notebook (although it's in Dutch), just make sure to change the output_folder to where you have data
