"""
settings.py: Settings script for banzai.

    Important note: due to the way that the parameters are read in,
    variables that begin with an underscore will not be added to the
    pipeline context.

"""
import os
import operator

from banzai.utils.instrument_utils import InstrumentCriterion
from banzai.utils.file_utils import ccdsum_to_filename, filter_to_filename


def telescope_to_filename(image):
    return image.header.get('TELESCOP', '').replace('-', '')


FRAME_SELECTION_CRITERIA = [InstrumentCriterion('type', operator.contains, 'FLOYDS', exclude=True),
                            InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]

FRAME_CLASS = 'banzai.images.Image'

ORDERED_STAGES = ['banzai.bpm.BPMUpdater',
                  'banzai.qc.HeaderSanity',
                  'banzai.qc.ThousandsTest',
                  'banzai.qc.SaturationTest',
                  'banzai.bias.OverscanSubtractor',
                  'banzai.crosstalk.CrosstalkCorrector',
                  'banzai.gain.GainNormalizer',
                  'banzai.mosaic.MosaicCreator',
                  'banzai.trim.Trimmer',
                  'banzai.bias.BiasSubtractor',
                  'banzai.dark.DarkSubtractor',
                  'banzai.flats.FlatDivider',
                  'banzai.qc.PatternNoiseDetector',
                  'banzai.photometry.SourceDetector',
                  'banzai.astrometry.WCSSolver',
                  'banzai.qc.pointing.PointingTest']

CALIBRATION_MIN_FRAMES = {'BIAS': 5,
                          'DARK': 5,
                          'SKYFLAT': 5}

CALIBRATION_SET_CRITERIA = {'BIAS': ['ccdsum'],
                            'DARK': ['ccdsum'],
                            'SKYFLAT': ['ccdsum', 'filter']}

LAST_STAGE = {'BIAS': 'banzai.trim.Trimmer', 'DARK': 'banzai.bias.BiasSubtractor', 'SKYFLAT': 'banzai.dark.DarkSubtractor',
              'SINISTRO': 'banzai.mosaic.MosaicCreator', 'STANDARD': None, 'EXPOSE': None}

EXTRA_STAGES = {'BIAS': ['banzai.bias.BiasMasterLevelSubtractor', 'banzai.bias.BiasComparer'],
                'DARK': ['banzai.dark.DarkNormalizer', 'banzai.dark.DarkComparer'],
                'SKYFLAT': ['banzai.flats.FlatNormalizer', 'banzai.qc.PatternNoiseDetector', 'banzai.flats.FlatComparer'],
                'STANDARD': None,
                'EXPOSE': None}

CALIBRATION_STACKER_STAGE = {'BIAS': 'banzai.bias.BiasMaker',
                             'DARK': 'banzai.dark.DarkMaker',
                             'SKYFLAT': 'banzai.flats.FlatMaker'}

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

REDIS_QUEUE_NAMES = {'DEFAULT': 'default',
                     'PROCESS_IMAGE': 'process_image',
                     'SCHEDULE_STACK': 'schedule_stack'}

CELERY_REDIS_HOST = 'redis://redis:6379/0'
CELERY_IMPORTS = ('banzai.main', 'banzai.celery')
CELERY_PREFETCH_MULTIPLIER = 1
CELERYD_MAX_TASKS_PER_CHILD = 1000


def make_calibration_filename_function(calibration_type, attribute_filename_functions, telescope_filename_function):
    def get_calibration_filename(image):
        name_components = {'site': image.site, 'telescop': telescope_filename_function(image),
                           'camera': image.header.get('INSTRUME', ''), 'epoch': image.epoch,
                           'cal_type': calibration_type.lower()}
        cal_file = '{site}{telescop}-{camera}-{epoch}-{cal_type}'.format(**name_components)
        for filename_function in attribute_filename_functions:
            cal_file += '-{}'.format(filename_function(image))
        cal_file += '.fits'
        return cal_file
    return get_calibration_filename


CALIBRATION_FILENAME_FUNCTIONS = {'BIAS': make_calibration_filename_function('BIAS', [ccdsum_to_filename], telescope_to_filename),
                                  'DARK': make_calibration_filename_function('DARK', [ccdsum_to_filename], telescope_to_filename),
                                  'SKYFLAT': make_calibration_filename_function('SKYFLAT', [ccdsum_to_filename, filter_to_filename],
                                                                                telescope_to_filename)}
