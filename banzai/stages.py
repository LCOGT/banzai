from __future__ import absolute_import, division, print_function, unicode_literals

import itertools
import importlib

from banzai.images import Image
from banzai.utils import image_utils
from banzai import dbs
from banzai import logs
import abc

__author__ = 'cmccully'

logger = logs.get_logger(__name__)

ordered_stages = ['munge.DataMunger',
                  'qc.ThousandsTest',
                  'qc.SaturationTest',
                  'bias.OverscanSubtractor',
                  'crosstalk.CrosstalkCorrector',
                  'gain.GainNormalizer',
                  'mosaic.MosaicCreator',
                  'bpm.BPMUpdater',
                  'trim.Trimmer',
                  'bias.BiasSubtractor',
                  'dark.DarkSubtractor',
                  'flats.FlatDivider',
                  'photometry.SourceDetector',
                  'astrometry.WCSSolver',
                  'pointing.PointingTest']


def get_stages_todo(last_stage=None, extra_stages=[]):
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
    The HeaderUpdater stage is always added at the very end.
    If last_stage is None then take all of the stages
    """
    stages_todo = []
    if last_stage is None:
        last_index = None
    else:
        last_index = ordered_stages.index(last_stage) + 1

    for stage_name in ordered_stages[:last_index] + extra_stages:
        importlib.import_module(stage_name, package='banzai')
        stages_todo.append(eval(stage_name))

    return stages_todo


class Stage(object):
    __metaclass__ = abc.ABCMeta

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    @abc.abstractproperty
    def group_by_keywords(self):
        pass

    def __init__(self, pipeline_context):
        self.logger = logs.get_logger(self.stage_name)
        self.pipeline_context = pipeline_context

    def get_grouping(self, image):
        grouping_criteria = [image.site, image.instrument, image.epoch]
        if self.group_by_keywords:
            grouping_criteria += [image.header[keyword] for keyword in self.group_by_keywords]
        return grouping_criteria

    def run_stage(self, image_set, image_config):
        image_set = list(image_set)
        tags = logs.image_config_to_tags(image_set[0], self.group_by_keywords)
        self.logger.info('Running {0}'.format(self.stage_name), extra=tags)
        return self.do_stage(image_set)

    def run(self, images):
        images.sort(key=self.get_grouping)
        processed_images = []
        for image_config, image_set in itertools.groupby(images, self.get_grouping):
            try:
                processed_images += self.run_stage(image_set, image_config)
            except MasterCalibrationDoesNotExist as e:
                continue
        return processed_images

    @abc.abstractmethod
    def do_stage(self, images):
        pass


class CalibrationMaker(Stage):
    __metaclass__ = abc.ABCMeta

    def __init__(self, pipeline_context):
        super(CalibrationMaker, self).__init__(pipeline_context)

    @abc.abstractproperty
    def calibration_type(self):
        pass

    @abc.abstractmethod
    def make_master_calibration_frame(self, images, image_config, logging_tags):
        pass

    @abc.abstractproperty
    def min_images(self):
        pass

    def do_stage(self, images):
        if len(images) < self.min_images:
            # Do nothing
            self.logger.warning('Not enough images to combine.')
            return []
        else:
            image_config = image_utils.check_image_homogeneity(images)
            logging_tags = logs.image_config_to_tags(image_config, self.group_by_keywords)

            return self.make_master_calibration_frame(images, image_config, logging_tags)

    def get_calibration_filename(self, image):
        cal_file = '{cal_type}_{instrument}_{epoch}_bin{bin}{filter}.fits'
        if 'filter' in self.group_by_keywords:
            filter_str = '_{filter}'.format(filter=image.filter)
        else:
            filter_str = ''

        cal_file = cal_file.format(instrument=image.instrument,
                                   epoch=image.epoch, bin=image.ccdsum.replace(' ', 'x'),
                                   cal_type=self.calibration_type.lower(), filter=filter_str)
        return cal_file


class MasterCalibrationDoesNotExist(Exception):
    pass


class ApplyCalibration(Stage):
    __metaclass__ = abc.ABCMeta

    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)

    @abc.abstractproperty
    def calibration_type(self):
        pass

    def do_stage(self, images):
        if len(images) == 0:
            # Abort!
            return []
        else:
            image_config = image_utils.check_image_homogeneity(images)
            logging_tags = logs.image_config_to_tags(image_config, self.group_by_keywords)
            master_calibration_filename = self.get_calibration_filename(images[0])

            if master_calibration_filename is None:
                self.logger.error('Master Calibration file does not exist for {stage}'.format(stage=self.stage_name),
                                  extra=logging_tags)
                raise MasterCalibrationDoesNotExist

            master_calibration_image = Image(self.pipeline_context,
                                             filename=master_calibration_filename)
            return self.apply_master_calibration(images, master_calibration_image, logging_tags)

    @abc.abstractmethod
    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        pass

    def get_calibration_filename(self, image):
        return dbs.get_master_calibration_image(image, self.calibration_type, self.group_by_keywords,
                                                db_address=self.pipeline_context.db_address)
