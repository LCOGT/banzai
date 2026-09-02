from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from banzai.utils import stage_utils


@pytest.mark.parametrize('start_stage, last_stage, extra_stages, expected', [
    (None, None, None, ['first', 'second', 'third']),
    ('second', None, None, ['second', 'third']),
    ('second', 'second', ['extra'], ['second', 'extra']),
])
def test_get_stages_for_individual_frame(start_stage, last_stage, extra_stages, expected):
    assert stage_utils.get_stages_for_individual_frame(
        ['first', 'second', 'third'],
        start_stage=start_stage,
        last_stage=last_stage,
        extra_stages=extra_stages,
    ) == expected


@pytest.mark.parametrize('reduction_level, expected_stages', [
    (0, ['first', 'source-detector', 'last']),
    (45, ['source-detector', 'last']),
])
def test_run_pipeline_stages_uses_start_stage_for_reduction_level(monkeypatch, reduction_level, expected_stages):
    image = MagicMock(meta={'RLEVEL': reduction_level}, obstype='EXPOSE')
    frame_factory = MagicMock()
    frame_factory.open.return_value = image
    stages_run = []

    def import_attribute(attribute):
        if attribute == 'frame-factory':
            return MagicMock(return_value=frame_factory)

        def stage_constructor(runtime_context):
            stage = MagicMock()
            stage.run.side_effect = lambda images: images
            stages_run.append(attribute)
            return stage

        return stage_constructor

    monkeypatch.setattr(stage_utils.import_utils, 'import_attribute', import_attribute)
    runtime_context = SimpleNamespace(
        FRAME_FACTORY='frame-factory',
        ORDERED_STAGES=['first', 'source-detector', 'last'],
        START_STAGE_BY_REDUCTION_LEVEL={'45': 'source-detector'},
        LAST_STAGE={'EXPOSE': None},
        EXTRA_STAGES={'EXPOSE': None},
    )

    stage_utils.run_pipeline_stages([{'path': 'test.fits'}], runtime_context)

    assert stages_run == expected_stages
    image.write.assert_called_once_with(runtime_context)
