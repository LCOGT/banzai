0.9.3 (2018-05-10)
------------------
- Hotfix
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
