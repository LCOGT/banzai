import operator
import abc

from banzai.context import InstrumentCriterion
from banzai import qc, bias, crosstalk, gain, mosaic, bpm, trim, dark, flats, photometry, astrometry, images
from banzai.utils.file_utils import ccdsum_to_filename, filter_to_filename


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

    @property
    @abc.abstractmethod
    def CALIBRATION_FILENAME_FUNCTIONS(self):
        pass

    @property
    @abc.abstractmethod
    def CALIBRATION_IMAGE_TYPES(self):
        pass

    @property
    @abc.abstractmethod
    def LAST_STAGE(self):
        pass

    @property
    @abc.abstractmethod
    def EXTRA_STAGES(self):
        pass

    SCHEDULABLE_CRITERIA = [InstrumentCriterion('schedulable', operator.eq, True)]


class ImagingSettings(Settings):

    FRAME_SELECTION_CRITERIA = [InstrumentCriterion('type', operator.contains, 'FLOYDS', exclude=True),
                                InstrumentCriterion('type', operator.contains, 'NRES', exclude=True)]

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

    CALIBRATION_MIN_IMAGES = {'BIAS': 5,
                              'DARK': 5,
                              'SKYFLAT': 5}

    CALIBRATION_SET_CRITERIA = {'BIAS': ['ccdsum'],
                                'DARK': ['ccdsum'],
                                'SKYFLAT': ['ccdsum', 'filter']}

    CALIBRATION_FILENAME_FUNCTIONS = {'BIAS': [ccdsum_to_filename],
                                      'DARK': [ccdsum_to_filename],
                                      'SKYFLAT': [ccdsum_to_filename, filter_to_filename]}

    LAST_STAGE = {'BIAS': trim.Trimmer, 'DARK': bias.BiasSubtractor, 'SKYFLAT': dark.DarkSubtractor,
                  'SINISTRO': mosaic.MosaicCreator, 'STANDARD': None, 'EXPOSE': None}

    EXTRA_STAGES = {'BIAS': [bias.BiasMasterLevelSubtractor, bias.BiasComparer, bias.BiasMaker],
                    'DARK': [dark.DarkNormalizer, dark.DarkComparer, dark.DarkMaker],
                    'SKYFLAT': [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer, flats.FlatMaker],
                    'STANDARD': None,
                    'EXPOSE': None}

    EXTRA_STAGES_PREVIEW = {'BIAS': [bias.BiasMasterLevelSubtractor, bias.BiasComparer],
                            'DARK': [dark.DarkNormalizer, dark.DarkComparer],
                            'SKYFLAT': [flats.FlatNormalizer, qc.PatternNoiseDetector, flats.FlatComparer],
                            'STANDARD': None,
                            'EXPOSE': None}

    CALIBRATION_IMAGE_TYPES = ['BIAS', 'DARK', 'SKYFLAT']

    SINISTRO_IMAGE_TYPES = ['BIAS', 'DARK', 'SKYFLAT', 'EXPOSE', 'STANDARD', 'TRAILED', 'EXPERIMENTAL']

    PREVIEW_ELIGIBLE_SUFFIXES = ['e00.fits', 's00.fits', 'b00.fits', 'd00.fits', 'f00.fits']
