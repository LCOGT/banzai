import sys
import logging
import operator

from lcogt_logging import LCOGTFormatter

from banzai.context import TelescopeCriterion
from banzai import qc, bias, crosstalk, gain, mosaic, bpm, trim, dark, flats, photometry, astrometry


# Logger set up
logging.captureWarnings(True)

# Set up the root logger
root_logger = logging.getLogger()
root_handler = logging.StreamHandler(sys.stdout)

# Add handler
formatter = LCOGTFormatter()
root_handler.setFormatter(formatter)
root_handler.setLevel(getattr(logging, 'DEBUG'))
root_logger.addHandler(root_handler)

# Stage and image type settings

IMAGING_CRITERIA = [TelescopeCriterion('camera_type', operator.contains, 'FLOYDS', exclude=True),
                    TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)]

SCHEDULABLE_CRITERIA = [TelescopeCriterion('schedulable', operator.eq, True)]

ORDERED_STAGES = [qc.HeaderSanity,
                  qc.ThousandsTest,
                  qc.SaturationTest,
                  bias.OverscanSubtractor,
                  crosstalk.CrosstalkCorrector,
                  gain.GainNormalizer,
                  mosaic.MosaicCreator,
                  bpm.BPMUpdater,
                  trim.Trimmer,
                  bias.BiasSubtractor,
                  dark.DarkSubtractor,
                  flats.FlatDivider,
                  qc.PatternNoiseDetector,
                  photometry.SourceDetector,
                  astrometry.WCSSolver,
                  qc.pointing.PointingTest]


BIAS_IMAGE_TYPES = ['BIAS']
BIAS_SUFFIXES = ['b00.fits']
BIAS_LAST_STAGE = trim.Trimmer
BIAS_EXTRA_STAGES = [bias.BiasMasterLevelSubtractor, bias.BiasComparer, bias.BiasMaker]
BIAS_EXTRA_STAGES_PREVIEW = [bias.BiasMasterLevelSubtractor, bias.BiasComparer]

DARK_IMAGE_TYPES = ['DARK']
DARK_SUFFIXES = ['d00.fits']
DARK_LAST_STAGE = bias.BiasSubtractor
DARK_EXTRA_STAGES = [dark.DarkNormalizer, dark.DarkComparer, dark.DarkMaker]
DARK_EXTRA_STAGES_PREVIEW = [dark.DarkNormalizer, dark.DarkComparer]

FLAT_IMAGE_TYPES = ['SKYFLAT']
FLAT_SUFFIXES = ['f00.fits']
FLAT_LAST_STAGE = dark.DarkSubtractor
FLAT_EXTRA_STAGES = [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer, flats.FlatMaker]
FLAT_EXTRA_STAGES_PREVIEW = [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer]

TRAILED_IMAGE_TYPES = ['TRAILED']

EXPERIMENTAL_IMAGE_TYPES = ['EXPERIMENTAL']

SINISTRO_IMAGE_TYPES = ['EXPOSE', 'STANDARD', 'BIAS', 'DARK', 'SKYFLAT', 'TRAILED', 'EXPERIMENTAL']
SINISTRO_LAST_STAGE = mosaic.MosaicCreator

SCIENCE_IMAGE_TYPES = ['EXPOSE', 'STANDARD']
SCIENCE_SUFFIXES = ['e00.fits', 's00.fits']

PREVIEW_ELIGIBLE_SUFFIXES = SCIENCE_SUFFIXES + BIAS_SUFFIXES + DARK_SUFFIXES + FLAT_SUFFIXES
