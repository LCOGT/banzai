from banzai.utils import import_utils
from collections import Iterable
import logging

logger = logging.getLogger('banzai')


def get_stages_for_individual_frame(ordered_stages, last_stage=None, extra_stages=None):
    """

    Parameters
    ----------
    ordered_stages: list of banzai.stages.Stage objects
    last_stage: banzai.stages.Stage
                Last stage to do
    extra_stages: Stages to do after the last stage

    Returns
    -------
    stages_todo: list of strings
                 The stages that need to be done: should of type banzai.stages.Stage

    Notes
    -----
    Extra stages can be other stages that are not in the ordered_stages list.
    """
    if extra_stages is None:
        extra_stages = []

    if last_stage is None:
        last_index = None
    else:
        last_index = ordered_stages.index(last_stage) + 1

    stages_todo = [stage for stage in ordered_stages[:last_index]]
    stages_todo += [stage for stage in extra_stages]

    return stages_todo


def run_pipeline_stages(image_paths, runtime_context):
    frame_factory = import_utils.import_attribute(runtime_context.FRAME_FACTORY)
    if isinstance(image_paths, list):
        images = [frame_factory.open(image_path, runtime_context) for image_path in image_paths]
        images = [image for image in images if image is not None or not image.is_raw()]
        if len(images) == 0:
            return
        stages_to_do = runtime_context.CALIBRATION_STACKER_STAGES[images[0].obstype.upper()]
    else:
        images = frame_factory.open(image_paths, runtime_context)
        stages_to_do = get_stages_for_individual_frame(runtime_context.ORDERED_STAGES,
                                                       last_stage=runtime_context.LAST_STAGE[images.obstype.upper()],
                                                       extra_stages=runtime_context.EXTRA_STAGES[images.obstype.upper()])
        if images is None or not images.is_raw():
            return

    for stage_name in stages_to_do:
        stage_constructor = import_utils.import_attribute(stage_name)
        stage = stage_constructor(runtime_context)
        images = stage.run(images)

        if images is None:
            logger.error('Reduction stopped', extra_tags={'filename': image_paths})
            return

    if isinstance(images, Iterable):
        for image in images:
            image.write(runtime_context)
    else:
        images.write(runtime_context)
