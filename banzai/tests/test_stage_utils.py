from unittest.mock import MagicMock

import pytest

from banzai.tests.utils import FakeContext
from banzai.utils import stage_utils


@pytest.mark.parametrize('start_stage, last_stage, extra_stages, skip_stages, expected', [
    (None, None, None, None, ['first', 'second', 'third']),
    ('second', None, None, None, ['second', 'third']),
    ('second', 'second', ['extra'], None, ['second', 'extra']),
    ('second', 'third', ['extra'], ['second', 'extra'], ['third']),
])
def test_get_stages_for_individual_frame(start_stage, last_stage, extra_stages, skip_stages, expected):
    assert stage_utils.get_stages_for_individual_frame(
        ['first', 'second', 'third'],
        start_stage=start_stage,
        last_stage=last_stage,
        extra_stages=extra_stages,
        skip_stages=skip_stages,
    ) == expected


@pytest.mark.parametrize('obstype, reduction_level, expected_stages', [
    ('SUB_EXP', 0, ['BadPixelMaskLoader', 'ReadNoiseLoader', 'SaturatedPixelFlagger', 'HeaderChecker',
                    'SaturationTest', 'OverscanSubtractor', 'CrosstalkCorrector', 'GainNormalizer',
                    'MosaicCreator', 'Trimmer', 'BiasSubtractor', 'PoissonInitializer', 'DarkSubtractor',
                    'FlatDivider']),
    ('EXPOSE', 0, ['BadPixelMaskLoader', 'ReadNoiseLoader', 'SaturatedPixelFlagger', 'HeaderChecker',
                   'ThousandsTest', 'SaturationTest', 'OverscanSubtractor', 'CrosstalkCorrector', 'GainNormalizer',
                   'MosaicCreator', 'Trimmer', 'BiasSubtractor', 'PoissonInitializer', 'DarkSubtractor',
                   'FlatDivider', 'PatternNoiseDetector', 'CosmicRayDetector', 'SourceDetector', 'WCSSolver',
                   'PointingTest', 'PhotometricCalibrator']),
    ('EXPOSE', 45, ['SourceDetector', 'WCSSolver', 'PointingTest', 'PhotometricCalibrator']),
])
def test_run_pipeline_stages_selects_stages_by_obstype_and_reduction_level(monkeypatch, obstype, reduction_level,
                                                                        expected_stages):
    image = MagicMock(meta={'RLEVEL': reduction_level}, obstype=obstype)
    frame_factory = MagicMock()
    frame_factory.open.return_value = image
    stages_run = []

    def import_attribute(attribute):
        if attribute == 'frame-factory':
            return MagicMock(return_value=frame_factory)

        def stage_constructor(runtime_context):
            stage = MagicMock()
            stage.run.side_effect = lambda images: images
            stages_run.append(attribute.rsplit('.', 1)[-1])
            return stage

        return stage_constructor

    monkeypatch.setattr(stage_utils.import_utils, 'import_attribute', import_attribute)
    runtime_context = FakeContext(FRAME_FACTORY='frame-factory')

    stage_utils.run_pipeline_stages([{'path': 'test.fits'}], runtime_context)

    assert stages_run == expected_stages
    image.write.assert_called_once_with(runtime_context)
