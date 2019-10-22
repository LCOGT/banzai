import logging
import abc
import os

import numpy as np
import astropy.io.fits as fits

from banzai.stages import Stage, MultiFrameStage
from banzai import dbs, logs
from banzai.utils import stats, qc, import_utils, stage_utils, fits_utils, date_utils, file_utils
from banzai.images import CCDData, LCOMasterCalibrationFrame, Section, stack

logger = logging.getLogger('banzai')


class CalibrationMaker(MultiFrameStage):
    def __init__(self, runtime_context):
        super(CalibrationMaker, self).__init__(runtime_context)

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
            return []
        if len(images) < min_images:
            # Do nothing
            msg = 'Number of images less than minimum requirement of {min_images}, not combining'
            logger.warning(msg.format(min_images=min_images))

        return [self.make_master_calibration_frame(images)]


class CalibrationStacker(CalibrationMaker):
    def __init__(self, runtime_context):
        super(CalibrationStacker, self).__init__(runtime_context)

    def make_master_calibration_frame(self, images):
        make_calibration_name = file_utils.make_calibration_filename_function(self.calibration_type,
                                                                              self.runtime_context)

        master_calibration_filename = make_calibration_name(images[0])

        master_image = LCOMasterCalibrationFrame(images, master_calibration_filename)
        # Split the image into N sections where N is the number of images
        # This is just for convenience. Technically N can be anything you want.
        # I assume that you can read a couple of images into memory so order N sections is good for memory management.
        N = len(images)

        # Split along the y-direction for faster indexing
        # detector section (y_stop - y_start) // binning(y) // N, abs(x_stop - x_start) // binning(x)
        y_step = images[0].shape[0] // N

        for i in range(N + 1):
            y_start = 1 + i * y_step

            if i == N:
                # Don't forget to do the last %mod sized section
                y_stop = images[0].shape[0]
            else:
                y_stop = (i + 1) * y_step

            section_to_stack = Section(x_start=1, x_stop=images[0].shape[1],
                                       y_start=y_start, y_stop=y_stop)

            data_to_stack = [image.primary_hdu[section_to_stack] for image in images]
            master_image.primary_hdu.copy_in(stack(data_to_stack, 3.0))

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
        master_calibration_filename = self.get_calibration_filename(image)

        if master_calibration_filename is None:
            return self.on_missing_master_calibration(image)

        frame_factory = import_utils.import_attribute(self.runtime_context.FRAME_FACTORY)

        master_calibration_image = frame_factory.open(master_calibration_filename, self.runtime_context)
        master_calibration_image.is_master = True
        logger.info('Applying master calibration', image=image,
                    extra_tags={'master_calibration': os.path.basename(master_calibration_filename)})
        return self.apply_master_calibration(image, master_calibration_image)

    @abc.abstractmethod
    def apply_master_calibration(self, image, master_calibration_image):
        pass

    def get_calibration_filename(self, image):
        return dbs.get_master_calibration_image(image, self.calibration_type, self.master_selection_criteria,
                                                use_only_older_calibrations=self.runtime_context.use_only_older_calibrations,
                                                db_address=self.runtime_context.db_address)


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
        bad_pixel_fraction = np.abs(image.data - master_calibration_image.data)
        # Estimate the noise of the image
        bad_pixel_fraction /= image.noise.add(master_calibration_image.noise)
        bad_pixel_fraction = bad_pixel_fraction >= self.SIGNAL_TO_NOISE_THRESHOLD
        bad_pixel_fraction = bad_pixel_fraction.sum() / float(bad_pixel_fraction.size)
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
                  'enclosure': instrument.enclosure, 'telescope': instrument.telescope,
                  'camera': instrument.camera, 'obstype': frame_type,
                  'min_date': min_date,
                  'max_date': max_date}
    logger.info("Making master frames", extra_tags=extra_tags)
    image_path_list = dbs.get_individual_calibration_images(instrument, frame_type, min_date, max_date,
                                                            db_address=runtime_context.db_address)
    if len(image_path_list) == 0:
        logger.info("No calibration frames found to stack", extra_tags=extra_tags)
    try:
        stage_utils.run_pipeline_stages(image_path_list, runtime_context)
    except Exception:
        logger.error(logs.format_exception())
    logger.info("Finished")
