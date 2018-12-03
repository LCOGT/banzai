import operator


class Settings:

    SCHEDULABLE_CRITERIA = [('schedulable', operator.eq, True)]

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


class Imaging(Settings):

    INSTRUMENT_CRITERIA = [('camera_type', operator.contains, 'FLOYDS', True),
                           ('camera_type', operator.contains, 'NRES', True)]

    IMAGE_CLASS = ('images', 'Image')

    ORDERED_STAGES = [('bpm', 'BPMUpdater'),
                      ('qc', 'HeaderSanity'),
                      ('qc', 'ThousandsTest'),
                      ('qc', 'SaturationTest'),
                      ('bias', 'OverscanSubtractor'),
                      ('crosstalk', 'CrosstalkCorrector'),
                      ('gain', 'GainNormalizer'),
                      ('mosaic', 'MosaicCreator'),
                      ('trim', 'Trimmer'),
                      ('bias', 'BiasSubtractor'),
                      ('dark', 'DarkSubtractor'),
                      ('flats', 'FlatDivider'),
                      ('qc', 'PatternNoiseDetector'),
                      ('photometry', 'SourceDetector'),
                      ('astrometry', 'WCSSolver'),
                      ('qc', 'PointingTest')]

    BIAS_LAST_STAGE = ('trim', 'Trimmer')
    BIAS_EXTRA_STAGES = [('bias', 'BiasMasterLevelSubtractor'), ('bias', 'BiasComparer'), ('bias', 'BiasMaker')]
    BIAS_EXTRA_STAGES_PREVIEW = [('bias', 'BiasMasterLevelSubtractor'),  ('bias', 'BiasComparer')]

    DARK_LAST_STAGE = ('bias', 'BiasSubtractor')
    DARK_EXTRA_STAGES = [('dark', 'DarkNormalizer'), ('dark', 'DarkComparer'), ('dark', 'DarkMaker')]
    DARK_EXTRA_STAGES_PREVIEW = [('dark', 'DarkNormalizer'), ('dark', 'DarkComparer')]

    FLAT_LAST_STAGE = ('dark', 'DarkSubtractor')
    FLAT_EXTRA_STAGES = [('flats', 'FlatNormalizer'), ('qc', 'PatternNoiseDetector'), ('flats', 'FlatComparer'),
                         ('flats', 'FlatMaker')]
    FLAT_EXTRA_STAGES_PREVIEW = [('flats', 'FlatNormalizer'), ('qc', 'PatternNoiseDetector'), ('flats', 'FlatComparer')]

    SINISTRO_LAST_STAGE = ('mosaic', 'MosaicCreator')

    CALIBRATION_MIN_IMAGES = 5
    GROUP_BY_ATTRIBUTES = {
        'BIAS': ['ccdsum'],
        'DARK': ['ccdsum'],
        'SKYFLAT': ['ccdsum', 'filter']
    }
