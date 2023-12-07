2.1.0 / 2023-12-06
------------------

New release, major changes.

General: 

- Refactoerd DB connection
- Refactored secrets handling
- Refactored xams context
- Added new minianalysis
- Fixed deprecated docs building framework

Processing plugins:

>>Peak processing
- Restructured plugins
- Fixed area fraction top calculation
- Peak classification works well, moved to peak_basics (to tune with options)
- Introduced gain correction (to tune with options, for now)
- Introduced position reconstruction with center of gravity

>>Events processing
- Restructured plugins
- Added simple S2 area correction for electron lifetime (to tune with options, for now)
- Added positions at event level
- Added new peak info at event level: waveform and area per channel

Processing framework:
- New files for processing, copying, deleting data..
- Moved all auto processing to cluster



2.0.0 / 2023-08-29
------------------

New release, major changes.

General:

- Adjust imports
- Add arguments to amstraxer for auto processing
- New amstraxer_easy for easier autoprocessing (removed unnecessary options..) -> TODO replace amstraxer
- autoprocessing/autoprocessing.py : add local processing option (for now preferred?)
- autoprocessing/process_run.py : add number of failures handling
- make amstrax straxen independent (to be discussed, for me is just source of problems, straxen changes quickly)
- contexts.py : adjusted some contexts options, TODO: clean all contexts
- rundb interface more structured
- added process_run as script

Processing plugins:

- refactored structure
- make plugins straxen free (at the cost of copy pasting few functions from straxen)
- unified raw records in one type only
- simplified pulse processing
- two new useful plugins for LED calibrations, see the full pulses
- workflow up to peaks tested, need events to tune things

1.2.0 / 2022-01-14
------------------
Major changes:
- Fix off by one error in the channel map of XAMS (#91)
- Fix off by one error in the channel map of XAMS (#92, #95)
- Updated HISTORY.md again for (same, but more complete) new release (#71)
- A bunch bumped requirements by dependabot (#72, #74, #78, #79, #83, #84, #85, #86, #87)


1.1.2 / 2021-12-23
------------------
 Since release v1.0.2, some things changed, of which the most important is the script that copies data to the stoomboot cluster.

-  Made a file that automatically copies new data files to stoomboot (#51)

Other merged PRs are:

- Add small utility to amstrax for version printing (#48)
- Set default to something that is produced (#49)
- fix line endings (#53) 
- Add readthedocs instructions (#67) 
- A lot of bumped versions in requirements.txt (PR #58 t/m #66)
- Fix docs (#68) 
- Update HISTORY.md with new release (#71) 

Notes:
- Previous release was a consequence of mflierm's clumsyness.


1.0.3 / 2021-12-23
------------------
 - Made a file that automatically copies new data files to stoomboot (#51)


1.0.2 / 2021-12-07
------------------
 - Bugfix XAMS context (#46)


1.0.1 / 2021-12-02
------------------
**BUG** for xams context, please use v1.0.2
- Flip the channel map for xams (#44)
- Few tweaks when submitting to stoomboot (#45)


1.0.0 / 2021-12-02
------------------
Stable version after first [`amstrax` project](https://github.com/XAMS-nikhef/amstrax/projects/1)

Breaking changes:
- Only `raw_records_v1724` and `raw_records_v1730` are provided. Plugins will have to be added in later versions
- `bootstrax` is deleted and replaced by simpler `amstraxer.py`-functionality
- Restructure of package
- Removal of notebooks to [dedicated repo](https://github.com/XAMS-nikhef/amstrax_notebooks)

Major updates:
- [New testing suite](https://github.com/XAMS-nikhef/amstrax/projects/1#column-17081786)
- [New integrations](https://github.com/XAMS-nikhef/amstrax/projects/1#column-17082170)
- [New DAQReader](https://github.com/XAMS-nikhef/amstrax/projects/1#column-17082394)
- [Simplified and streamlined auto processing](https://github.com/XAMS-nikhef/amstrax/projects/1#column-17082384)
- [Documentation](https://github.com/XAMS-nikhef/amstrax/projects/1#column-17082172) on [new documentation website](https://amstrax.readthedocs.io/en/latest/?badge=latest)

Related pull requests:
- Add badges to Amstrax and update readme (#23)
- Plugin, context and package structure (#24)
- Fix autoprocessing scripts (#25)
- Delete bootstrax (#26)
- remove notebooks (#27)
- Add documentation
- Add context test (#28)
- Code cleanup (#36, #40)
- Fix straxen requirements for testing (#37)
- Add init for autoprocessing (#39)
- Test autoprocessing scripts (#41)
- Add documentation (#42, #43)


0.1.0 / 2021-12-02
--------------------
- Add testing and restructure amstrax (#13)

0.0.1 / <2021-12-01
--------------------
