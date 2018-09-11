0.9.9 (2018-09-11)
------------------
- Added filters to only reduce imaging files. These can be overridden for the FLOYDS and NRES pipelines.

0.9.8 (2018-08-30)
------------------
- Refactored exceptions for missing bad pixel masks
- Added fallback check to search the TELESCOP keyword in the configdb (necessary for NRES)
- Added override to the bad pixel mask requirement
- Integrated e2e testing 
    - This test must be ignored when running pytest locally by using the option "-m 'not e2e'"
- Modified pattern noise QC check to ignore large-scale pattern features
- Added try/catch blocks to fail more gracefully if images are the incorrect size

0.9.7 (2018-08-22)
------------------
- Modified pattern noise QC check to reduce false positives 
- Enabled rejection of bias and dark frames when creating masters
- Pinned pytest due to recursion depth issue
- Bias level subtractor now subtracts the mean of the images rather than the value from previous masters.

0.9.6 (2018-07-23)
------------------
- Added functions to check whether image is 3d, and to extract central portion of image 
- Updated Read the Docs

0.9.5 (2018-06-11)
------------------
- Fixed header checker
- Fixed typo in master dark comparison equation

0.9.4 (2018-05-17)
------------------
- Fixed a bug that would stop preview frames from being retried if they failed even once.
- Hotfix to remove double division by exposure time when creating master darks
- Fixed bug that prevented calibration comparison being run on skyflats
- Fixed image class null case 

0.9.3 (2018-05-10)
------------------
- Hotfix (temporary until pattern noise and calibration comparer parameters are
  tuned to avoid false positives)
    - No longer removes images that fail pattern noise test
    - Bias comparer no longer run in master bias creator 

0.9.2 (2018-05-08)
------------------
- Standardized elasticsearch field names

0.9.1 (2018-05-07)
------------------
- Minor bugfix

0.9.0 (2018-05-07)
------------------
- Add comparison stages for calibration frames
    - Master calibration frames now go through the preview pipeline
    - Each new calibration is compared to the most recent master frame
      which should alert us if the camera is having issues 
- Refactored the Stage class to include quality control helpers

0.8.1 (2018-04-24)
------------------
- Upgraded to newest version of the astropy package template
- Added continuous integration in Jenkins
- Now only Python 3 compatible as we use astropy 3.0

0.8.0 (2018-04-23)
------------------
- Stable version with many bugfixes
- Last Python 2 compatible release
- Last commit before many new quality control tests

0.7.9 (2016-03-25)
------------------
- Initial Release
