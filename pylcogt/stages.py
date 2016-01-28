from __future__ import absolute_import, print_function, division

import os
import itertools

from sqlalchemy.sql import func

from . import dbs
from .utils import date_utils
from . import logs

import abc

logger = logs.get_logger(__name__)

__author__ = 'cmccully'


def make_output_directory(pipeline_context, image_config):
    # Create output directory if necessary
    output_directory = os.path.join(pipeline_context.processed_path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    return


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

    def run(self, images):
        for image_config, image_set in itertools.groupby(images, self.get_grouping):
            make_output_directory(self.pipeline_context, image_config)
            tags = logs.image_config_to_tags(image_config)
            self.logger.info('Running {0}'.format(self.stage_name), extra=tags)
            self.do_stage(list(image_set))

    @abc.abstractmethod
    def do_stage(self, images):
        pass


class CalibrationMaker(Stage):
    def __init__(self, pipeline_context):
        super(CalibrationMaker, self).__init__(pipeline_context)

    def get_calibration_filename(self, image):
        output_directory = os.path.join(self.pipeline_context.processed_path)
        cal_file = '{filepath}/{cal_type}_{instrument}_{epoch}_bin{bin}{filter}.fits'
        if 'filter' in self.group_by_keywords:
            filter_str = '_{filter}'.format(filter=image.filter)
        else:
            filter_str = ''

        cal_file = cal_file.format(filepath=output_directory, instrument=image.instrument,
                                   epoch=image.epoch, bin=image.ccdsum.replace(' ', 'x'),
                                   cal_type=image.header['OBSTYPE'].lower(), filter=filter_str)
        return cal_file


class ApplyCalibration(Stage):
    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)


    def get_calibration_filename(self, epoch, telescope, image_config):
        calibration_criteria = dbs.CalibrationImage.type == self.cal_type.upper()
        calibration_criteria &= dbs.CalibrationImage.telescope_id == telescope.id

        for criteria in self.group_by:
            group_by_field = vars(criteria)['key']
            calibration_criteria &= getattr(dbs.CalibrationImage, group_by_field) == getattr(image_config,
                                                                                             group_by_field)

        db_session = dbs.get_session()

        calibration_query = db_session.query(dbs.CalibrationImage).filter(calibration_criteria)
        epoch_datetime = date_utils.epoch_string_to_date(epoch)

        find_closest = func.DATEDIFF(epoch_datetime, dbs.CalibrationImage.dayobs)
        find_closest = func.ABS(find_closest)

        calibration_query = calibration_query.order_by(find_closest.asc())
        calibration_image = calibration_query.first()
        calibration_file = os.path.join(calibration_image.filepath, calibration_image.filename)

        return calibration_file
