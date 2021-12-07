1.7.0 (2021-12-07)
------------------
- Bugfix to read in non-fpacked files

1.6.0 (2021-11-01)
------------------
- Added Full-Width Tenth Maximum statistics
- We now calculate the FWHM using contours rather than using the shape parameters from SEP (which were strongly correlated with flux.)

1.5.7 (2021-10-20)
------------------
- Fix to work with gaia-astrometry-service version 0.6.0
- Add filename to catalog payload
- Remove reference to solve ID

1.5.6 (2021-10-18)
------------------
- Increased the buffer size for SEP
- Minor bugfixes

1.5.5 (2021-10-05)
------------------
- Configure celery tasks to automatically retry in-progress tasks in the case of a container failure or scale-down

1.5.4 (2021-09-21)
------------------
- Add more granular error handling for fitting photometry

1.5.3 (2021-09-21)
------------------
- Additional fixes for photometric calibrations

1.5.2 (2021-09-16)
------------------
- Fix for photometric calibration for images with no WCS solution

1.5.1 (2021-09-14)
------------------
- Fix for photometric catalog service URL

1.5.0 (2021-09-13)
------------------
- Added ccd temperature as a matching criteria for super darks.

1.4.0 (2021-09-13)
------------------
- Added photometric calibration in the g, r, i, and z filters using the ATLAS-REFCAT2 catalog.

1.3.6 (2021-09-13)
------------------
- Added a limit to the 200 brightest sources to be sent to the astrometry service to keep the service from timing out.

1.3.5 (2021-08-02)
------------------
- Added new public LCO standard observation proposals to settings

1.3.4 (2021-06-21)
------------------
- Restriped arrays to be accessed in row-major order when stacking to improve performance.

1.3.3 (2021-05-26)
------------------
- Add EXPERIMENTAL obstype to list of supported obstypes.

1.3.2 (2021-05-24)
------------------
- Add custom celery task queue routing. 

1.3.1 (2021-05-24)
------------------
- Fix bug where reduction would fail if image catalog did not exist.

1.3.0 (2021-04-26)
------------------
- Upgraded infrastructure on how data products are saved/stored for downstream users.

1.2.1 (2021-04-08)
------------------
- Upgrade to ocs-ingester 2.3.0, which adds the ability to ingest data products of 
  arbitrary type to an OCS Science Archive

1.2.0 (2021-04-04)
------------------
- Added the option to losslessly compress fits extensions.

1.1.4 (2020-03-23)
-------------------
- Update to use OCS Ingester 2.2.6, with fixes for NRES data products.

1.1.3 (2020-02-23)
-------------------
- Move public CI from Travis to GitHub Actions

1.1.2 (2020-01-14)
-------------------
- Update to use OCS Ingester version 2.2.5

1.1.1 (2020-12-17)
-------------------
- Add documentation and example configuration to helm chart
- Fix bug where raw data settings would not properly default if 
  environment variables were not set

1.1.0 (2020-11-30)
-------------------
- Remove dependence on astropy-helpers for building and packaging
- Changes for AWS deployment of BANZAI-Imaging

1.0.6 (2020-08-13)
-------------------
- Add helm chart and development kubernetes deployment

1.0.5 (2020-07-17)
-------------------
- Changed format of multi-amplifier OVERSCAN keyword to be more FITS compliant

1.0.4 (2020-07-16)
-------------------
- Add MuSCAT Data to end-to-end tests
- Fix gain normalization bug, where the gain header keywird was not adjusted properly upon image
normalization.
- Fix bug which caused OVERSCAN keyword to be discarded for multi-amplifier frames.

1.0.3 (2020-07-13)
-------------------
- Adjust to new ConfigDB structure which uses multiple science cameras
per instrument.

1.0.2 (2020-07-13)
-------------------
- Fix for sqlalchemy DB connections not getting cleaned up

1.0.1 (2020-07-13)
-------------------
- Add latitude, longitude and elevation to site database model.

1.0.0 (2020-06-23)
-------------------
- Bump to new major version. Reduced images now include uncertainties.

0.29.0 (2020-03)
-------------------
- Refactored Image class. Cleaned up a lot of plumbing code

0.28.9 (2020-03-18)
-------------------
- Fix master calibration image selection to not use frames marked as bad.

0.28.8 (2020-03-09)
-------------------
- Upgrade ingester lib version to address OpenTSDB default HTTP port issue.
- Fix EXPTIME to be a float, rounded to 6 digits.

0.28.7 (2020-03-05)
-------------------
- Explicitly truncate exposure time to 6 decimal places, since the Archive API will not accept EXPTIME values with more decimal places.

0.28.6 (2020-03-02)
-------------------
- Update retrieving individual calibration image records to exclude master calibrations.
  In some cases, manual stacking of masters was picking up old master calibrations and including
  them in new master cals.

0.28.5 (2020-02-18)
-------------------
- Update lco-ingester version to 2.1.11 to add extra metrics tag.

0.28.4 (2020-02-12)
-------------------
- Add initial crosstalk coefficients for fa19

0.28.3 (2020-02-11)
-------------------
- Fixes for ChunkedEncodingErrors when downloading files from s3:
- Add retry and exponential backoff to s3 file downloading
- Add `stream=True` flag to `requests.get()` call when downloading from s3
- Adds dependency on tenacity==6.0.0 for retry logic

0.28.2 (2020-02-06)
-------------------
- Fix for parsing RLEVEL from archived_fits message
- Fix for metrics collection

0.28.1 (2020-01-27)
-------------------
- Update lco-ingester to latest version (2.1.9)

0.28.0 (2020-01-23)
-------------------
- Migrate BANZAI to be compatible with s3. Frames will now be downloaded from the LCO Archive, and posted
  directly to the ingester, bypassing LCO's `/archive` machine. 

0.27.6 (2020-01-13)
-------------------
- Update celery task visibility timeout to 24h to avoid re-scheduling stacking tasks that do not complete within an hour. 
  This addresses the issue of creating multiple calibration stacks within seconds of each other.
- https://docs.celeryproject.org/en/latest/getting-started/brokers/redis.html#id1

0.27.5 (2019-12-11)
-------------------
- Change ra, dec parsing to default to CRVAL header keywords. Ref. Redmine issue #1104.

0.27.4 (2019-11-13)
-------------------
- Fix for parsing instruments with empty string codes from ConfigDB

0.27.3 (2019-11-04)
-------------------
- Fix for retrieving correct number of calibration blocks from observation portal.

0.27.2 (2019-11-01)
-------------------
- Fixes for celerybeat scheduling of calibration stacking checks

0.27.1 (2019-10-30)
-------------------
- Update scheduling of calibration stacking to use observation portal, as lake is being retired.

0.27.0 (2019-07-25)
-------------------
- Refactored configuration management to make it possible to override by BANZAI-NRES

0.26.8 (2019-08-06)
-------------------
- Add ability to automatically reduce data from instruments in STANDBY state.

0.26.7 (2019-07-24)
-------------------
- Fixed a typo in the photometry stage

0.26.6 (2019-07-23)
-------------------
- We now do not include sources with a NaN FWHM in the catalogs.

0.26.5 (2019-07-22)
-------------------
- Bug fix for ignore schedulability in call to need_to_process inside of process_image

0.26.4 (2019-07-19)
-------------------
- Fix for uncaught exceptions in astrometry stage

0.26.3 (2019-07-17)
-------------------
- Updates end-to-end tests to mock the lake

0.26.2 (2019-07-11)
-------------------
- Fixes for WCSERR keyword population

0.26.1 (2019-06-25)
-------------------
- Fixes for bad pixel masks.
- If a frame causes an uncaught exception, stop the reduction

0.26.0 (2019-06-17)
-------------------
- Fixes for named readout deployment

0.25.0 (2019-06-14)
-------------------
- Added name instrument table to support NRES

0.24.0 (2019-06-06)
-------------------
- Added support for named configuration modes

0.23.3 (2019-06-04)
-------------------
- Added an explicit check to make sure the exposure time is not zero in dark frames before stacking.

0.23.2 (2019-05-22)
-------------------
- Made broker-url a required argument for all console entry points

0.23.1 (2019-05-21)
-------------------
- Fixed a bug in stacking master calibrations from the command line.
- Fixed logging format to conform to LCO standard format and to not be overridden by celery
- Fixed a bug that would use calibration frames to stack even if they were marked as bad

0.23.0 (2019-05-08)
-------------------
- Changed license from BSD to GPLv3, the standard LCO license

0.22.0 (2019-05-05)
-------------------
- Significant refactor to how BANZAI runs. BANZAI now runs via celery tasks.
- Calibration stacking is now scheduled by checking the Lake for calibration 
  blocks.
  
0.21.0 (2019-03-25)
-------------------
- Significant refactor to the pipeline context and settings files. We have now
  split settings that are static into the settings file and settings that can
  change at runtime into the "runtime context". This is in preparation for 
  running a task queue (e.g. dramatiq).

0.20.1 (2019-03-14)
-------------------
- Fixed bug where gaia astrometry service solve failure was not handled properly

0.20.0 (2019-03-11)
-------------------
- Configure pipeline to use GAIA-Astrometry.net service for WCS solves.

0.19.4 (2019-03-05)
-------------------
- We no longer overwrite all SATURATE vales for the 0.4m telescopes. We now only use the
  defaults if the value is missing or 0. We also set/check MAXLIN to the same value.
- Require enclosure and telescope for instrument query
- Avoid trying to reduce files that don't have fits extension in filename
- Added entrypoint to update instruments table in database
- Removed defunct entrypoint to reduce night by site

0.19.3 (2019-02-13)
-------------------
- Fixed bug where master calibrations had wrong daydir in filename

0.19.2 (2019-02-12)
-------------------
- Bugfix in parsing arguments for the real-time processing

0.19.1 (2019-02-11)
-------------------
- Removed `'epoch'` from list of parameters to check for image 
  homogeneity 
- Changed how image homogeneity is checked so that pipeline does
  not continue to run after check fails
- Refactored "preview" to "realtime" processing so that reduced files are placed in
  the correct directories

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
