import operator
import abc

from banzai.context import TelescopeCriterion
from banzai import qc, bias, crosstalk, gain, mosaic, bpm, trim, dark, flats, photometry, astrometry, images


class Settings(abc.ABC):

    @property
    @abc.abstractmethod
    def FRAME_SELECTION_CRITERIA(self):
        pass

    @property
    @abc.abstractmethod
    def FRAME_CLASS(self):
        pass

    @property
    @abc.abstractmethod
    def ORDERED_STAGES(self):
        pass

    @property
    @abc.abstractmethod
    def CALIBRATION_MIN_IMAGES(self):
        pass

    @property
    @abc.abstractmethod
    def CALIBRATION_SET_CRITERIA(self):
        pass

    SCHEDULABLE_CRITERIA = [TelescopeCriterion('schedulable', operator.eq, True)]

    BIAS_IMAGE_TYPES = ['BIAS']
    BIAS_SUFFIXES = ['b00.fits']

    DARK_IMAGE_TYPES = ['DARK']
    DARK_SUFFIXES = ['d00.fits']

    FLAT_IMAGE_TYPES = ['SKYFLAT']
    FLAT_SUFFIXES = ['f00.fits']

    SCIENCE_IMAGE_TYPES = ['EXPOSE', 'STANDARD']
    SCIENCE_SUFFIXES = ['e00.fits', 's00.fits']

    TRAILED_IMAGE_TYPES = ['TRAILED']

    EXPERIMENTAL_IMAGE_TYPES = ['EXPERIMENTAL']

    SINISTRO_IMAGE_TYPES = SCIENCE_IMAGE_TYPES + BIAS_IMAGE_TYPES + DARK_IMAGE_TYPES + FLAT_IMAGE_TYPES +\
        TRAILED_IMAGE_TYPES + EXPERIMENTAL_IMAGE_TYPES

    PREVIEW_ELIGIBLE_SUFFIXES = SCIENCE_SUFFIXES + BIAS_SUFFIXES + DARK_SUFFIXES + FLAT_SUFFIXES


class ImagingSettings(Settings):

    FRAME_SELECTION_CRITERIA = [TelescopeCriterion('camera_type', operator.contains, 'FLOYDS', exclude=True),
                                TelescopeCriterion('camera_type', operator.contains, 'NRES', exclude=True)]

    FRAME_CLASS = images.Image

    ORDERED_STAGES = [bpm.BPMUpdater,
                      qc.HeaderSanity,
                      qc.ThousandsTest,
                      qc.SaturationTest,
                      bias.OverscanSubtractor,
                      crosstalk.CrosstalkCorrector,
                      gain.GainNormalizer,
                      mosaic.MosaicCreator,
                      trim.Trimmer,
                      bias.BiasSubtractor,
                      dark.DarkSubtractor,
                      flats.FlatDivider,
                      qc.PatternNoiseDetector,
                      photometry.SourceDetector,
                      astrometry.WCSSolver,
                      qc.pointing.PointingTest]

    CALIBRATION_MIN_IMAGES = {
        'BIAS': 5,
        'DARK': 5,
        'SKYFLAT': 5,
    }

    CALIBRATION_SET_CRITERIA = {
        'BIAS': ['ccdsum'],
        'DARK': ['ccdsum'],
        'SKYFLAT': ['ccdsum', 'filter']
    }

    BIAS_LAST_STAGE = trim.Trimmer
    BIAS_EXTRA_STAGES = [bias.BiasMasterLevelSubtractor, bias.BiasComparer, bias.BiasMaker]
    BIAS_EXTRA_STAGES_PREVIEW = [bias.BiasMasterLevelSubtractor, bias.BiasComparer]

    DARK_LAST_STAGE = bias.BiasSubtractor
    DARK_EXTRA_STAGES = [dark.DarkNormalizer, dark.DarkComparer, dark.DarkMaker]
    DARK_EXTRA_STAGES_PREVIEW = [dark.DarkNormalizer, dark.DarkComparer]

    FLAT_LAST_STAGE = dark.DarkSubtractor
    FLAT_EXTRA_STAGES = [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer, flats.FlatMaker]
    FLAT_EXTRA_STAGES_PREVIEW = [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer]

    SINISTRO_LAST_STAGE = mosaic.MosaicCreator
