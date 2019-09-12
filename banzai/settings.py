"""
settings.py: Settings script for banzai.

    Important note: due to the way that the parameters are read in,
    variables that begin with an underscore will not be added to the
    pipeline context.

"""
import os

FRAME_SELECTION_CRITERIA = [('type', 'not contains', 'FLOYDS'), ('type', 'not contains', 'NRES')]

FRAME_CLASS = 'banzai.images.LCOImagingFrame'

# ORDERED_STAGES = ['banzai.bpm.BadPixelMaskLoader',
#                   #'banzai.bpm.SaturatedPixelFlagger',
#                   #'banzai.qc.HeaderChecker',
#                   #'banzai.qc.ThousandsTest',
#                   #'banzai.qc.SaturationTest',
#                   'banzai.bias.OverscanSubtractor',
#                   #'banzai.crosstalk.CrosstalkCorrector',
#                   'banzai.gain.GainNormalizer',
#                   'banzai.mosaic.MosaicCreator',
#                   'banzai.trim.Trimmer',
#                   'banzai.bias.BiasSubtractor',
#                   #'banzai.dark.DarkSubtractor',
#                   #'banzai.flats.FlatDivider',
#                   #'banzai.qc.PatternNoiseDetector',
#                   #'banzai.photometry.SourceDetector',
#                   #'banzai.astrometry.WCSSolver',
#                   #'banzai.qc.pointing.PointingTest']
ORDERED_STAGES = ['banzai.bpm.BadPixelMaskLoader']
#                  'banzai.bpm.SaturatedPixelFlagger',
#                  'banzai.bias.OverscanSubtractor',
#                  'banzai.gain.GainNormalizer',
#                  'banzai.mosaic.MosaicCreator',
#                  'banzai.trim.Trimmer',
#                  'banzai.bias.BiasSubtractor']

CALIBRATION_MIN_FRAMES = {'BIAS': 5,
                          'DARK': 5,
                          'SKYFLAT': 5}

CALIBRATION_SET_CRITERIA = {'BIAS': ['configuration_mode', 'ccdsum'],
                            'DARK': ['configuration_mode', 'ccdsum'],
                            'SKYFLAT': ['configuration_mode', 'ccdsum', 'filter']}

LAST_STAGE = {'BIAS': None, #'banzai.trim.Trimmer',
              'DARK': 'banzai.bias.BiasSubtractor', 'SKYFLAT': 'banzai.dark.DarkSubtractor',
              'SINISTRO': 'banzai.mosaic.MosaicCreator', 'STANDARD': None, 'EXPOSE': None}

EXTRA_STAGES = {'BIAS': None, # ['banzai.bias.BiasMasterLevelSubtractor', 'banzai.bias.BiasComparer'],
                'DARK': ['banzai.dark.DarkNormalizer', 'banzai.dark.DarkComparer'],
                'SKYFLAT': ['banzai.flats.FlatNormalizer', 'banzai.qc.PatternNoiseDetector', 'banzai.flats.FlatComparer'],
                'STANDARD': None,
                'EXPOSE': None}

CALIBRATION_STACKER_STAGES = {'BIAS': ['banzai.bias.BiasMaker'],
                              'DARK': ['banzai.dark.DarkMaker'],
                              'SKYFLAT': ['banzai.flats.FlatMaker']}

CALIBRATION_IMAGE_TYPES = ['BIAS', 'DARK', 'SKYFLAT']

# Stack delays are expressed in seconds--namely, each is five minutes
CALIBRATION_STACK_DELAYS = {'BIAS': 300,
                            'DARK': 300,
                            'SKYFLAT': 300}

SINISTRO_IMAGE_TYPES = ['BIAS', 'DARK', 'SKYFLAT', 'EXPOSE', 'STANDARD', 'TRAILED', 'EXPERIMENTAL']

SCHEDULE_STACKING_CRON_ENTRIES = {'coj': {'minute': 30, 'hour': 6},
                                  'cpt': {'minute': 0, 'hour': 15},
                                  'tfn': {'minute': 30, 'hour': 17},
                                  'lsc': {'minute': 0, 'hour': 21},
                                  'elp': {'minute': 0, 'hour': 23},
                                  'ogg': {'minute': 0, 'hour': 3}}

ASTROMETRY_SERVICE_URL = os.getenv('ASTROMETRY_SERVICE_URL', 'http://astrometry.lco.gtn/catalog/')

CALIBRATION_FILENAME_FUNCTIONS = {'BIAS': ('banzai.utils.file_utils.config_to_filename',
                                           'banzai.utils.file_utils.ccdsum_to_filename'),
                                  'DARK': ('banzai.utils.file_utils.config_to_filename',
                                           'banzai.utils.file_utils.ccdsum_to_filename'),
                                  'SKYFLAT': ('banzai.utils.file_utils.config_to_filename',
                                              'banzai.utils.file_utils.ccdsum_to_filename',
                                              'banzai.utils.file_utils.filter_to_filename')}

TELESCOPE_FILENAME_FUNCTION = 'banzai.utils.file_utils.telescope_to_filename'

LAKE_URL = os.getenv('LAKE_URL', 'http://lake.lco.gtn/blocks/')
CALIBRATE_PROPOSAL_ID = os.getenv('CALIBRATE_PROPOSAL_ID', 'calibrate')

CONFIGDB_URL = os.getenv('CONFIGDB_URL', 'http://configdb.lco.gtn/sites/')
