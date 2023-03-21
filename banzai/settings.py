"""
settings.py: Settings script for banzai.

    Important note: due to the way that the parameters are read in,
    variables that begin with an underscore will not be added to the
    pipeline context.

"""
import os
import banzai

FRAME_SELECTION_CRITERIA = [('type', 'not contains', 'FLOYDS'), ('type', 'not contains', 'NRES')]

FRAME_FACTORY = 'banzai.lco.LCOFrameFactory'

CALIBRATION_FRAME_CLASS = 'banzai.lco.LCOCalibrationFrame'

ORDERED_STAGES = ['banzai.bpm.BadPixelMaskLoader',
                  'banzai.bias.OverscanSubtractor',
                  'banzai.gain.GainNormalizer',
                  'banzai.mosaic.MosaicCreator',
                  'banzai.trim.Trimmer',
                  'banzai.bias.BiasSubtractor',
                  'banzai.uncertainty.PoissonInitializer',
                  'banzai.flats.FlatDivider',
                  'banzai.cosmic.CosmicRayDetector',
                  'banzai.photometry.SourceDetector',
                  'banzai.astrometry.WCSSolver',
                  'banzai.qc.pointing.PointingTest',
                  'banzai.photometry.PhotometricCalibrator']

CALIBRATION_MIN_FRAMES = {'BIAS': 1,
                          'FLAT': 1}

CALIBRATION_SET_CRITERIA = {'BIAS': ['binning'],
                            'DARK': ['binning', 'ccd_temperature'],
                            'FLAT': ['binning', 'filter'],
                            'BPM': ['binning']}

LAST_STAGE = {'BIAS': 'banzai.trim.Trimmer',
              'DARK': 'banzai.uncertainty.PoissonInitializer', 'FLAT': 'banzai.dark.DarkSubtractor',
              'SINISTRO': 'banzai.mosaic.MosaicCreator', 'STANDARD': None, 'EXPOSE': None, 'EXPERIMENTAL': None}

EXTRA_STAGES = {'BIAS': ['banzai.bias.BiasMasterLevelSubtractor', 'banzai.bias.BiasComparer'],
                'DARK': ['banzai.dark.DarkNormalizer', 'banzai.dark.DarkTemperatureChecker',
                         'banzai.dark.DarkComparer'],
                'FLAT': ['banzai.flats.FlatSNRChecker', 'banzai.flats.FlatNormalizer', 'banzai.qc.PatternNoiseDetector',
                            'banzai.flats.FlatComparer'],
                'STANDARD': None,
                'EXPOSE': None,
                'EXPERIMENTAL': None}

CALIBRATION_STACKER_STAGES = {'BIAS': ['banzai.bias.BiasMaker'],
                              'DARK': ['banzai.dark.DarkMaker'],
                              'FLAT': ['banzai.flats.FlatMaker']}

CALIBRATION_IMAGE_TYPES = ['BIAS', 'FLAT']

# Stack delays are expressed in seconds--namely, each is five minutes
CALIBRATION_STACK_DELAYS = {'BIAS': 300,
                            'DARK': 300,
                            'FLAT': 300}

SINISTRO_IMAGE_TYPES = ['BIAS', 'DARK', 'FLAT', 'EXPOSE', 'STANDARD', 'TRAILED', 'EXPERIMENTAL']

SCHEDULE_STACKING_CRON_ENTRIES = {'coj': {'minute': 30, 'hour': 6},
                                  'cpt': {'minute': 0, 'hour': 15},
                                  'tfn': {'minute': 30, 'hour': 17},
                                  'lsc': {'minute': 0, 'hour': 21},
                                  'elp': {'minute': 0, 'hour': 23},
                                  'ogg': {'minute': 0, 'hour': 3}}

ASTROMETRY_SERVICE_URL = os.getenv('ASTROMETRY_SERVICE_URL', 'http://astrometry.lco.gtn/catalog/')

CALIBRATION_FILENAME_FUNCTIONS = {'BIAS': ['banzai.utils.file_utils.ccdsum_to_filename'],
                                  'FLAT': ['banzai.utils.file_utils.ccdsum_to_filename',
                                           'banzai.utils.file_utils.filter_to_filename']}

TELESCOPE_FILENAME_FUNCTION = 'banzai.utils.file_utils.telescope_to_filename'

OBSERVATION_PORTAL_URL = os.getenv('OBSERVATION_PORTAL_URL',
                                   'http://internal-observation-portal.lco.gtn/api/observations/')

# Specify different sources of raw and processed data, if needed.
ARCHIVE_API_ROOT = os.getenv('API_ROOT')
ARCHIVE_AUTH_TOKEN = os.getenv('AUTH_TOKEN')
ARCHIVE_FRAME_URL = f'{ARCHIVE_API_ROOT}frames'
if ARCHIVE_AUTH_TOKEN is None:
    ARCHIVE_AUTH_HEADER = None
else:
    ARCHIVE_AUTH_HEADER = {'Authorization': f'Token {ARCHIVE_AUTH_TOKEN}'}

RAW_DATA_AUTH_TOKEN = os.getenv('RAW_DATA_AUTH_TOKEN', ARCHIVE_AUTH_TOKEN)
RAW_DATA_API_ROOT = os.getenv('RAW_DATA_API_ROOT', ARCHIVE_API_ROOT)
RAW_DATA_FRAME_URL = f'{RAW_DATA_API_ROOT}frames'
if RAW_DATA_AUTH_TOKEN is None:
    RAW_DATA_AUTH_HEADER = None
else:
    RAW_DATA_AUTH_HEADER = {'Authorization': f'Token {RAW_DATA_AUTH_TOKEN}'}

CALIBRATE_PROPOSAL_ID = os.getenv('CALIBRATE_PROPOSAL_ID', 'calibrate')
FITS_EXCHANGE = os.getenv('FITS_EXCHANGE', 'archived_fits')

CONFIGDB_URL = os.getenv('CONFIGDB_URL', 'http://configdb.lco.gtn/sites/')

# The Observation Portal is not always consistent with OBSTYPE in the header so this maps any differences
# If an observation type is not in this list, we assume it is the same in the portal and the header
OBSERVATION_REQUEST_TYPES = {}

# For some extension names, we want to just have corresponding BPM or ERR extensions
EXTENSION_NAMES_TO_CONDENSE = ['SCI', 'UNKNOWN']

CALIBRATION_LOOKBACK = {'BIAS': 0.5, 'DARK': 0.5, 'FLAT': 0.5}

PIPELINE_VERSION = banzai.__version__

# Number of days before proprietary data should become public:
DATA_RELEASE_DELAY = 365

# Proposal ids for data that should be public instantly. Should all be lowercase
PUBLIC_PROPOSALS = ['calibrate', 'standard', '*standards', '*epo*', 'pointing']

SUPPORTED_FRAME_TYPES = ['BPM', 'READNOISE', 'BIAS', 'DARK', 'FLAT', 'EXPOSE', 'STANDARD', 'EXPERIMENTAL']

REDUCED_DATA_EXTENSION_ORDERING = {'BIAS': ['SCI', 'BPM', 'ERR'],
                                   'DARK': ['SCI', 'BPM', 'ERR'],
                                   'FLAT': ['SCI', 'BPM', 'ERR'],
                                   'EXPOSE': ['SCI', 'CAT', 'BPM', 'ERR'],
                                   'STANDARD': ['SCI', 'CAT', 'BPM', 'ERR'],
                                   'EXPERIMENTAL': ['SCI', 'CAT', 'BPM', 'ERR']}

MASTER_CALIBRATION_EXTENSION_ORDER = {'BIAS': ['SCI', 'BPM', 'ERR'],
                                      'DARK': ['SCI', 'BPM', 'ERR'],
                                      'FLAT': ['SCI', 'BPM', 'ERR']}

REDUCED_DATA_EXTENSION_TYPES = {'SCI': 'float32',
                                'ERR': 'float32',
                                'BPM': 'uint8'}

LOSSLESS_EXTENSIONS = []

CELERY_TASK_QUEUE_NAME = os.getenv('CELERY_TASK_QUEUE_NAME', 'celery')

REFERENCE_CATALOG_URL = os.getenv('REFERENCE_CATALOG_URL', 'http://phot-catalog.lco.gtn/')
