import sys
import logging

from lcogt_logging import LCOGTFormatter

from banzai import bias, dark, flats, trim, photometry, astrometry, qc, crosstalk, gain, mosaic, bpm


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
BIAS_LAST_STAGE = trim.Trimmer
BIAS_EXTRA_STAGES = [bias.BiasMasterLevelSubtractor, bias.BiasComparer, bias.BiasMaker]
BIAS_EXTRA_STAGES_PREVIEW = [bias.BiasMasterLevelSubtractor, bias.BiasComparer]

DARK_IMAGE_TYPES = ['DARK']
DARK_LAST_STAGE = bias.BiasSubtractor
DARK_EXTRA_STAGES = [dark.DarkNormalizer, dark.DarkComparer, dark.DarkMaker]
DARK_EXTRA_STAGES_PREVIEW = [dark.DarkNormalizer, dark.DarkComparer]

FLAT_IMAGE_TYPES = ['SKYFLAT']
FLAT_LAST_STAGE = dark.DarkSubtractor
FLAT_EXTRA_STAGES = [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer, flats.FlatMaker]
FLAT_EXTRA_STAGES_PREVIEW = [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer]

TRAILED_IMAGE_TYPES = ['TRAILED']

EXPERIMENTAL_IMAGE_TYPES = ['EXPERIMENTAL']

SINISTRO_IMAGE_TYPES = ['EXPOSE', 'STANDARD', 'BIAS', 'DARK', 'SKYFLAT', 'TRAILED', 'EXPERIMENTAL']
SINISTRO_LAST_STAGE = mosaic.MosaicCreator

SCIENCE_IMAGE_TYPES = ['EXPOSE', 'STANDARD']

PREVIEW_IMAGE_TYPES = ['EXPOSE', 'STANDARD', 'BIAS', 'DARK', 'SKYFLAT']
PREVIEW_ELIGIBLE_SUFFIXES = ['e00.fits', 's00.fits', 'b00.fits', 'd00.fits', 'f00.fits']


def get_stages_todo(last_stage=None, extra_stages=None):
    """

    Parameters
    ----------
    last_stage: banzai.stages.Stage
                Last stage to do
    extra_stages: Stages to do after the last stage

    Returns
    -------
    stages_todo: list of banzai.stages.Stage
                 The stages that need to be done

    Notes
    -----
    Extra stages can be other stages that are not in the ordered_stages list.
    """
    if extra_stages is None:
        extra_stages = []

    if last_stage is None:
        last_index = None
    else:
        last_index = ORDERED_STAGES.index(last_stage) + 1

    stages_todo = ORDERED_STAGES[:last_index] + extra_stages
    return stages_todo


def get_preview_stages_todo(image_suffix):
    if image_suffix == 'b00.fits':
        stages = get_stages_todo(last_stage=BIAS_LAST_STAGE,
                                 extra_stages=BIAS_EXTRA_STAGES_PREVIEW)
    elif image_suffix == 'd00.fits':
        stages = get_stages_todo(last_stage=DARK_LAST_STAGE,
                                 extra_stages=DARK_EXTRA_STAGES_PREVIEW)
    elif image_suffix == 'f00.fits':
        stages = get_stages_todo(last_stage=FLAT_LAST_STAGE,
                                 extra_stages=FLAT_EXTRA_STAGES_PREVIEW)
    else:
        stages = get_stages_todo()
    return stages
