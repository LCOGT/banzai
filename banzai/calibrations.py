import abc
from datetime import datetime

from banzai.stages import Stage
from banzai import dbs, logs
from banzai.utils import qc, import_utils, stage_utils, file_utils
from banzai.data import stack
from banzai.utils.image_utils import Section

logger = logs.get_logger()


class CalibrationMaker(Stage):
    def __init__(self, runtime_context):
        super(CalibrationMaker, self).__init__(runtime_context)

    @property
    def group_by_attributes(self):
        return self.runtime_context.CALIBRATION_SET_CRITERIA.get(self.calibration_type.upper(), [])

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    @abc.abstractmethod
    def make_master_calibration_frame(self, images):
        pass

    def do_stage(self, images):
        try:
            min_images = self.runtime_context.CALIBRATION_MIN_FRAMES[self.calibration_type.upper()]
        except KeyError:
            msg = 'The minimum number of frames required to create a master calibration of type ' \
                  '{calibration_type} has not been specified in the settings.'
            logger.error(msg.format(calibration_type=self.calibration_type.upper()))
            return None
        if len(images) < min_images:
            # Do nothing
            msg = 'Number of images less than minimum requirement of {min_images}, not combining'
            logger.warning(msg.format(min_images=min_images))
            return None

        return self.make_master_calibration_frame(images)


class CalibrationStacker(CalibrationMaker):
    def __init__(self, runtime_context):
        super(CalibrationStacker, self).__init__(runtime_context)

    @property
    def process_by_group(self):
        return True

    def make_master_calibration_frame(self, images):
        make_calibration_name = file_utils.make_calibration_filename_function(self.calibration_type,
                                                                              self.runtime_context)

        # use the most recent image in the stack to create the master filename
        master_calibration_filename = make_calibration_name(max(images, key=lambda x: datetime.strptime(x.epoch, '%Y%m%d') ))

        grouping = self.runtime_context.CALIBRATION_SET_CRITERIA.get(images[0].obstype, [])
        master_frame_class = import_utils.import_attribute(self.runtime_context.CALIBRATION_FRAME_CLASS)
        hdu_order = self.runtime_context.MASTER_CALIBRATION_EXTENSION_ORDER.get(self.calibration_type)

        master_image = master_frame_class.init_master_frame(images, master_calibration_filename,
                                                            grouping_criteria=grouping, hdu_order=hdu_order)

        # turn off memory mapping for each segment
        for image in images:
            image.primary_hdu.memmap = False
        # Split the image into N sections where N is the number of images
        # This is just for convenience. Technically N can be anything you want.
        # I assume that you can read a couple of images into memory so order N sections is good for memory management.
        N = len(images)
        # Split along the y-direction to get more sequential reads off of disk
        # detector section (y_stop - y_start) // binning(y) // N, abs(x_stop - x_start) // binning(x)
        y_step = images[0].shape[0] // N
        for i in range(N + 1):
            y_start = 1 + i * y_step
            if i == N:
                # If the image divided evenly just move on.
                if images[0].shape[0] % N == 0:
                    break
                # Otherwise, don't forget to do the last %mod sized section
                y_stop = images[0].shape[0]
            else:
                y_stop = (i + 1) * y_step

            section_to_stack = Section(x_start=1, x_stop=images[0].data.shape[1],
                                       y_start=y_start, y_stop=y_stop)

            data_to_stack = [image.primary_hdu[section_to_stack] for image in images]
            stacked_data = stack(data_to_stack, 3.0)

            master_image.primary_hdu.copy_in(stacked_data)

        logger.info('Created master calibration stack', image=master_image,
                    extra_tags={'calibration_type': self.calibration_type})
        return master_image


class CalibrationUser(Stage):
    def __init__(self, runtime_context):
        super(CalibrationUser, self).__init__(runtime_context)

    @property
    def master_selection_criteria(self):
        return self.runtime_context.CALIBRATION_SET_CRITERIA.get(self.calibration_type.upper(), [])

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    def on_missing_master_calibration(self, image):
        logger.error('Master {caltype} does not exist'.format(caltype=self.calibration_type.upper()), image=image)
        if self.runtime_context.override_missing:
            return image
        else:
            return None

    def do_stage(self, image):
        master_calibration_file_info = self.get_calibration_file_info(image)
        if master_calibration_file_info is None:
            return self.on_missing_master_calibration(image)

        frame_factory = import_utils.import_attribute(self.runtime_context.FRAME_FACTORY)()
        master_calibration_image = frame_factory.open(master_calibration_file_info, self.runtime_context)
        master_calibration_image.is_master = True
        logger.info('Applying master calibration', image=image,
                    extra_tags={'master_calibration':  master_calibration_image.filename})
        return self.apply_master_calibration(image, master_calibration_image)

    @abc.abstractmethod
    def apply_master_calibration(self, image, master_calibration_image):
        pass

    def get_calibration_file_info(self, image):
        return dbs.cal_record_to_file_info(
            dbs.get_master_cal_record(image, self.calibration_type, self.master_selection_criteria,
                                      self.runtime_context.db_address, 
                                      use_only_older_calibrations=self.runtime_context.use_only_older_calibrations,
                                      prefer_same_block=self.runtime_context.same_block_cals,
                                      prefer_same_proposal=self.runtime_context.prefer_same_proposal,
                                      check_public=self.runtime_context.check_public_cals)
        )


class CalibrationComparer(CalibrationUser):
    # In a 16 megapixel image, this should flag 0 or 1 pixels statistically, much much less than 5% of the image
    SIGNAL_TO_NOISE_THRESHOLD = 6.0
    ACCEPTABLE_PIXEL_FRACTION = 0.05

    def on_missing_master_calibration(self, image):
        logger.error('No master {caltype} to compare to, Flagging image as bad.'.format(caltype=self.calibration_type),
                     image=image)
        image.is_bad = True
        return image

    def is_frame_bad(self, image, master_calibration_image):
        # We assume the image has already been normalized before this stage is run.
        difference_image = image - master_calibration_image

        outliers = difference_image.signal_to_noise() >= self.SIGNAL_TO_NOISE_THRESHOLD
        bad_pixel_fraction = outliers.sum() / float(outliers.size)
        frame_is_bad = bad_pixel_fraction > self.ACCEPTABLE_PIXEL_FRACTION

        qc_results = {"master_comparison.fraction": bad_pixel_fraction,
                      "master_comparison.snr_threshold": self.SIGNAL_TO_NOISE_THRESHOLD,
                      "master_comparison.pixel_threshold": self.ACCEPTABLE_PIXEL_FRACTION,
                      "master_comparison.comparison_master_filename": master_calibration_image.filename,
                      "master_comparison.failed": frame_is_bad}

        qc.save_qc_results(self.runtime_context, qc_results, image)
        return frame_is_bad

    def apply_master_calibration(self, image, master_calibration_image):
        frame_is_bad = self.is_frame_bad(image, master_calibration_image)
        if frame_is_bad:
            image.is_bad = True
            msg = 'Flagging {caltype} as bad because it deviates too much from the previous master'
            logger.error(msg.format(caltype=self.calibration_type), image=image)
        return image


def make_master_calibrations(instrument, frame_type, min_date, max_date, runtime_context):
    extra_tags = {'type': instrument.type, 'site': instrument.site,
                  'camera': instrument.camera, 'obstype': frame_type,
                  'min_date': min_date,
                  'max_date': max_date}
    logger.info("Making master frames", extra_tags=extra_tags)
    calibration_frames_info = dbs.get_individual_cal_frames(instrument, frame_type, min_date, max_date,
                                                            db_address=runtime_context.db_address)
    if len(calibration_frames_info) == 0:
        logger.info("No calibration frames found to stack", extra_tags=extra_tags)
    try:
        stage_utils.run_pipeline_stages(calibration_frames_info, runtime_context, calibration_maker=True)
    except Exception:
        logger.error(logs.format_exception())
    logger.info("Finished")
