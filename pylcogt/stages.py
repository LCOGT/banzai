__author__ = 'cmccully'

import os
from sqlalchemy.sql import func
import itertools

from . import dbs
from .utils import date_utils
from . import logs

class Stage:
    def __init__(self, stage_function, master_cal_function='', processed_path='', initial_query=None):
        self.stage_function = stage_function
        self.db_session = dbs.get_session()
        self.master_cal_function = master_cal_function
        self.processed_path = processed_path

    def __del__(self):
        self.db_session.close()

    def select_input_images(self, telescope, epoch):
        # Select only the images we want to work on
        query = self.initial_query & (dbs.Image.telescope_id == telescope.id)
        query = query & (dbs.Image.dayobs == epoch)

        # Get the distinct values of ccdsum that we are using
        ccdsum_list = self.db_session.query(dbs.Image.ccdsum).filter(query).distinct()
        filter_list = self.db_session.query(dbs.Image.filter_name).filter(query).distinct()

        for image_config in itertools.product(ccdsum_list, filter_list):
            # Select only images with the correct binning
            config_query = query & (dbs.Image.ccdsum == image_config.ccdsum)
            config_query = config_query & (dbs.Image.filter_name == image_config.filter_name)
            config_list = self.db_session.query(dbs.Image).filter(config_query).all()
            # Convert from image objects to file names
            input_image_list = []
            for image in config_list:
                input_image_list.append(os.path.join(image.filepath, image.filename))

        return input_image_list

    # By default don't return any output images
    def get_output_images(self, telescope, epoch):
       return None


    def make_output_directory(self, telescope, epoch):
            # Create output directory if necessary
            output_directory = os.path.join(self.processed_path, telescope.site,
                                            telescope.instrument, epoch)
            if not os.path.exists(output_directory):
                os.makedirs(output_directory)

    # By default we don't need to get a calibration image
    def get_calibration_image(self, epoch, ccdsum, cal_type):
        return None


    def run(self, epoch_list, telescope_list):
        for epoch, telescope in itertools.product(epoch_list, telescope_list):
            self.make_output_directory(telescope, epoch)

            image_sets, image_configs = self.select_input_images(telescope, epoch)
            logger = logs.get_logger(self.cal_type)

            for images, image_config in zip(image_sets, image_configs):
                log_message = log_message.format(binning=image_config.ccdsum.replace(' ','x'),
                                                 instrument=telescope.instrument, epoch=epoch)
                logger.info(log_message)

                stage_args = [images]

                output_images = self.get_output_images()
                if output_images is not None:
                    stage_args.append(output_images)

                master_cal_file = self.get_master_cal(epoch, image_config.ccdsum)
                if master_cal_file is not None:
                    stage_args.append(master_cal_file)

                self.stage_function(*stage_args)


class MakeCalibrationImage(Stage):
    def __init__(self):
        super(MakeCalibrationImage, self).__init__()

class ApplyCalibration(Stage):
    def __init__(self):
        super(ApplyCalibration, self).__init__()

    def get_output_images(self, telescope, epoch):
        return self.select_input_images(telescope, epoch)

    def get_calibration_image(self, epoch, ccdsum, cal_type):
        calibration_query = self.db_session.query(dbs.Calibration_Image)
        calibration_query = calibration_query.filter(dbs.Calibration_Image.type == cal_type)
        calibration_query = calibration_query.filter(dbs.Calibration_Image.ccdsum == ccdsum)

        epoch_datetime = date_utils.epoch_string_to_date(epoch)

        find_closest = func.DATEDIFF(epoch_datetime, dbs.Calibration_Image.dayobs)
        find_closest = func.ABS(find_closest)

        calibration_query = calibration_query.order_by(find_closest.desc())

        calibration_image = calibration_query.one()
        calibration_file = os.path.join(calibration_image.filepath, calibration_image.filename)

        return calibration_file