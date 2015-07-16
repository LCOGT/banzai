__author__ = 'cmccully'

import os
from sqlalchemy.sql import func, expression
import itertools

from . import dbs
from .utils import date_utils
from . import logs

class Stage(object):
    def __init__(self, stage_function, processed_path='', initial_query=None, groupby=[],
                 logger_name='', log_message='', cal_type=''):
        self.stage_function = stage_function
        self.processed_path = processed_path
        self.initial_query = initial_query
        self.groupby = groupby
        self.db_session = dbs.get_session()
        self.logger_name = logger_name
        self.log_message = log_message
        self.cal_type = cal_type

    def __del__(self):
        self.db_session.close()

    def select_input_images(self, telescope, epoch):
        # Select only the images we want to work on
        query = self.initial_query & (dbs.Image.telescope_id == telescope.id)
        query = query & (dbs.Image.dayobs == epoch)

        if len(self.groupby) != 0:
            # Get the distinct values of ccdsum and filters
            config_query = self.db_session.query(*self.groupby)
            config_list = config_query.filter(query).distinct().all()

            config_queries = []

            for config in config_list:
                config_query = query
                for i in range(len(self.groupby)):
                    # Select images with the correct binning/filter
                    config_query = config_query & (self.groupby[i] == config[i])
                config_queries.append(config_query)

        else:
            config_queries = [expression.true()]

        input_image_list = []
        config_list = []
        for image_config in config_queries:
            image_query = image_config & query

            image_list = self.db_session.query(dbs.Image).filter(image_query).all()

            # Convert from image objects to file names
            input_image_list.append(image_list)

            config_list.append(image_list[0])
        return input_image_list, config_list

    # By default don't return any output images
    def get_output_images(self, telescope, epoch):
       return None


    def make_output_directory(self, epoch, telescope):
            # Create output directory if necessary
            output_directory = os.path.join(self.processed_path, telescope.site,
                                            telescope.instrument, epoch)
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)


    # By default we don't need to get a calibration image
    def get_calibration_image(self, epoch, telescope, image_config):
        return None


    def run(self, epoch_list, telescope_list):
        for epoch, telescope in itertools.product(epoch_list, telescope_list):
            self.make_output_directory(epoch, telescope)

            image_sets, image_configs = self.select_input_images(telescope, epoch)
            logger = logs.get_logger(self.logger_name)

            for images, image_config in zip(image_sets, image_configs):
                log_message = self.log_message.format(instrument=telescope.instrument, epoch=epoch,
                                                      site=telescope.site, binning=image_config.ccdsum,
                                                      filter_name=image_config.filter_name)
                logger.info(log_message)

                stage_args = [images]

                output_images = self.get_output_images(telescope, epoch)
                if output_images is not None:
                    stage_args.append(output_images)

                master_cal_file = self.get_calibration_image(epoch, telescope, image_config)
                if master_cal_file is not None:
                    stage_args.append(master_cal_file)

                self.stage_function(*stage_args)


class MakeCalibrationImage(Stage):
    def __init__(self, stage_function, processed_path='', initial_query=None, groupby=[],
                 logger_name='', log_message='', cal_type=''):

        query = initial_query & (dbs.Image.obstype == cal_type)
        super(MakeCalibrationImage, self).__init__(stage_function, processed_path=processed_path,
                                                   initial_query=query, groupby=groupby,
                                                   logger_name=logger_name, log_message=log_message, cal_type=cal_type)

    def get_calibration_image(self, epoch, telescope, image_config):
        output_directory = os.path.join(self.processed_path, telescope.site, telescope.instrument, epoch)
        cal_file = '{filepath}/{cal_type}_{instrument}_{epoch}_bin{bin}{filter}.fits'
        if dbs.Image.filter_name in self.groupby:
            filter_str = '_{filter}'.format(filter = image_config.filter_name)
        else:
            filter_str = ''

        cal_file = cal_file.format(filepath=output_directory, instrument=telescope.instrument, epoch=epoch,
                                   bin=image_config.ccdsum.replace(' ','x'), cal_type=self.cal_type, filter=filter_str)
        return cal_file

    def get_output_images(self, telescope, epoch):
        return None


class ApplyCalibration(Stage):

    def __init__(self, stage_function, processed_path='', initial_query=None, groupby=[],
                 logger_name='', log_message='', cal_type=''):

        super(ApplyCalibration, self).__init__(stage_function, processed_path=processed_path,
                                                   initial_query=initial_query, groupby=groupby,
                                                   logger_name=logger_name, log_message=log_message, cal_type=cal_type)
    def get_output_images(self, telescope, epoch):
        return self.select_input_images(telescope, epoch)

    def get_calibration_image(self, epoch, ccdsum, cal_type, filter_name):
        calibration_query = self.db_session.query(dbs.Calibration_Image)
        calibration_query = calibration_query.filter(dbs.Calibration_Image.type == cal_type)
        calibration_query = calibration_query.filter(dbs.Calibration_Image.ccdsum == ccdsum)
        calibration_query = calibration_query.filter(dbs.Calibration_Image.filter_name == filter_name)

        epoch_datetime = date_utils.epoch_string_to_date(epoch)

        find_closest = func.DATEDIFF(epoch_datetime, dbs.Calibration_Image.dayobs)
        find_closest = func.ABS(find_closest)

        calibration_query = calibration_query.order_by(find_closest.desc())

        calibration_image = calibration_query.one()
        calibration_file = os.path.join(calibration_image.filepath, calibration_image.filename)

        return calibration_file