from banzai import settings
from banzai.utils import import_utils, image_utils, realtime_utils
import logging

logger = logging.getLogger('banzai')


# TODO: This module should be renamed and/or refactored. It was put in place to resolve an issue with circular imports
# in an expedient manner and should be given more attention.


def get_stages_todo(ordered_stages, last_stage=None, extra_stages=None):
    """

    Parameters
    ----------
    ordered_stages: list of banzai.stages.Stage objects
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
        last_index = ordered_stages.index(last_stage) + 1

    stages_todo = [import_utils.import_attribute(stage) for stage in ordered_stages[:last_index]]

    stages_todo += [import_utils.import_attribute(stage) for stage in extra_stages]

    return stages_todo


def run(file_info, runtime_context):
    """
    Main driver script for banzai.
    """
    #TODO: Update to use archive API
    image = image_utils.read_image(file_info, runtime_context)
    if image is None:
        return
    stages_to_do = get_stages_todo(settings.ORDERED_STAGES,
                                   last_stage=settings.LAST_STAGE[image.obstype],
                                   extra_stages=settings.EXTRA_STAGES[image.obstype])
    logger.info("Starting to reduce frame", image=image)
    for stage in stages_to_do:
        stage_to_run = stage(runtime_context)
        image = stage_to_run.run(image)
    if image is None:
        logger.error('Reduction stopped', extra_tags={'filename': realtime_utils.get_filename(file_info)})
        return
    image.write(runtime_context)
    logger.info("Finished reducing frame", image=image)
