0.19.0 (2019-02-07)
-------------------
- The `Stage` class and its inheritors have been changed to only process 
  one frame at a time
- Stages that require multiple frames (i.e. the calibration
  stackers) now inherit from the distinct `MultiFrameStage` class

0.18.4 (2019-02-07)
-------------------
- Moved the function that adds quality control information to ElasticSearch
  outside of Stage class and to the quality control utils

0.18.3 (2019-02-05)
-------------------
- Fixed query for master calibration to work even when block start
  time is null

0.18.2 (2019-02-01)
-------------------
- Added a catch for N/A in fits header dates
- Fixed a log message bug that causes a crash when a frame reduction fails

0.18.1 (2019-01-31)
-------------------
- Breaking typo in Preview Pipeline removed 

0.18.0 (2019-01-29)
-------------------
- Calibration stacking is now separate from data reduction. Individual
  calibration frames are reduced and added to the database. The stacking 
  method then queries the database using a range of dates to determine
  which frames should be stacked. 
- It is now possible to mark frames as good or bad in the database
- Individual calibration frames for which a previous good master to 
  perform a comparison against does not exist are marked as bad
  
0.17.2 (2019-01-24)
-------------------
- Increased the character limit of string columns in the database

0.17.1 (2019-01-23)
-------------------
- Added a creation date column to the `CalibrationImage` table

0.17.0 (2019-01-21)
-------------------
- Refactored settings.py to make it possible to override for BANZAI-NRES
- Various refactors and generalizations, like FRAME_CLASS that can be overridden by BANZAI-NRES

0.16.0 (2018-12-11)
-------------------
- Significant changes made to the database structure:
  - The `PreviewImage` table has been renamed to `ProcessedImage`
  - The `Telescope` table has been renamed to `Instrument`; the `instrument` 
    column is now `camera`; and the `camera_type` column is now `type`
  - `enclosure` and `telescope` columns have been added to the `Instrument` table
  - The `BadPixelMask` table has been removed, and BPMs are now located in the 
    `CalibrationImage` table as type `BPM`
  - In the `CalibrationImage` table, `dayobs` has been changed to `dateobs` and
    provides the date and time the observation took place; `telescope_id` has 
    been renamed to `instrument_id`; an `is_master` column has been added; 
    a JSON formatted `attributes` column is now used to store parameters such 
    as `ccdsum` and `filter` which no longer have their own dedicated columns;
    and an `is_bad` column has been added to mark bad calibration frames
- To reflect the name change of the `Telescope` table to `Instrument`, all
  `telescope` instances are now named `intrument`
- All calibration frames (individual and master) are saved to the 
  `CalibrationImage` table
- The naming scheme for master calibration files has been changed from:
  ```
  {cal_type}_{instrument}_{epoch}_bin{bin}_{filter}.fits
  ```
  to:
  ```
  {site}{telescope}-{camera}-{epoch}-{cal_type}-bin{bin}-{filter}.fits
  ```
- Functionality for migrating from an old format to a new format database has
  been added to `/banzai/utils/db_migration.py`

0.15.1 (2018-12-05)
-------------------
- Fixed AppplyCalibration class to still use group_by (broken since 0.14.1)
- Fixed typo that broke preview pipeline in 0.15.0

0.15.0 (2018-12-05)
-------------------
- Restructured settings to be an abstract class
- Methods in main.py must now specify which version of settings to use 
- All parameters from settings are now added to the pipeline context

0.14.4 (2018-11-28)
-------------------
- Fixed stages to return empty image list if an exception occured
- Fixed small logging typos

0.14.3 (2018-11-27)
-------------------
- Added catch to any logger errros to avoid crashing pipeline in case of logging
  message typos
- Fixed a logging message typo in image_utils

0.14.2 (2018-11-26)
-------------------
- If telescope isn't found in the database, parameters are populated from image header
- Fixed BPM filename header keyword check in BPM stage
- Fixed logging call in stages.py when image list is empty
- Fixed logging call in create_master_calibration_header when keyword cannot be added

0.14.1 (2018-11-13)
-------------------
- Added full traceback of uncaught exceptions to the logs
- Removed group_by_attributes property fom all stages except CalibrationMaker
- Added master_selection_criteria property to CalibrationComparer
  
0.14.0 (2018-11-13)
-------------------
- Refactored bias, dark, and flat makers to use a common superclass to remove
  code duplication.

0.13.0 (2018-11-12)
-------------------
- Refactored pipeline context so that we can subclass image types for BANZAI NRES.
- Fixed bug (introduced in 0.11.3) where reduce night would 
  only reduce data from a single telescope per site

0.12.1 (2018-11-08)
-------------------
- Refactored group_by_keywords to be group_by_attributes
- Images are now grouped by Image attributes rather than header keywords.

0.12.0 (2018-11-08)
-------------------
- Refactored BPM read-in and addition to occur in BPM stage instead of
  during image read-in
- Cast rlevel to an integer in command line arguments 

0.11.3 (2018-11-07)
-------------------
- Refactored calibration-related stages into their own module 
- Moved stage and image parameters from main to settings

0.11.2 (2018-11-01)
-------------------
- Fixed a bug in calling reduce_night
- Added back the ability to process individual science frames rather than a full directory.

0.11.1 (2018-10-31)
-------------------
- Fixed bug where preview mode was not set to true when running preview pipeline

0.11.0 (2018-10-30)
-------------------
- Added command-line option to ignore telescope schedulability requirement  

0.10.0 (2018-10-25)
-------------------
- Refactored how the pipeline context object works:
    - The pipeline context object is now immutable
    - The pipeline context object now has sensible defaults for the standard parameters.
    - Future parameters can be added to the pipeline context without requiring banzai core to edited.

0.9.12 (2018-10-15)
-------------------
- Created new logger class to add image-specific info to logging tags  
- Updated table to HDU conversion to use astropy's built in function.

0.9.11 (2018-10-02)
-------------------
- Added support for more tables to be associated with images (catalogs, etc.)
- Removed wavelet convolution from pattern noise QC check algorithm
- Modified photometry unit names to prevent astropy fits standard warnings
- Added pyyaml pacakge requirement to prevent warnings due to photometry tables 
  having description columns

0.9.10 (2018-09-13)
-------------------
-  Changed how the filters for camera classes are plumbed

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
