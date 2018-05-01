from __future__ import absolute_import, division, print_function, unicode_literals

import itertools
import elasticsearch

from banzai import dbs
from banzai import logs
from banzai.images import Image
from banzai.utils import image_utils
from banzai.qc.utils import format_qc_results

import abc

logger = logs.get_logger(__name__)

__author__ = 'cmccully'


class Stage(abc.ABC):

    def __init__(self, pipeline_context):
        self.logger = logs.get_logger(self.stage_name)
        self.pipeline_context = pipeline_context

    @property
    def stage_name(self):
        return '.'.join([__name__, self.__class__.__name__])

    @property
    @abc.abstractmethod
    def group_by_keywords(self):
        return []

    def get_grouping(self, image):
        grouping_criteria = [image.site, image.instrument, image.epoch]
        if self.group_by_keywords:
            grouping_criteria += [image.header[keyword] for keyword in self.group_by_keywords]
        return grouping_criteria

    def run_stage(self, image_set):
        image_set = list(image_set)
        tags = logs.image_config_to_tags(image_set[0], self.group_by_keywords)
        self.logger.info('Running {0}'.format(self.stage_name), extra=tags)
        return self.do_stage(image_set)

    def run(self, images):
        images.sort(key=self.get_grouping)
        processed_images = []
        for _, image_set in itertools.groupby(images, self.get_grouping):
            try:
                processed_images += self.run_stage(image_set)
            except MasterCalibrationDoesNotExist as e:
                continue
        return processed_images

    @abc.abstractmethod
    def do_stage(self, images):
        return images

    def save_qc_results(self, qc_results, image, **kwargs):
        """
        Save the Quality Control results to ElasticSearch

        Parameters
        ----------
        qc_results : dict
                     Dictionary of key value pairs to be saved to ElasticSearch
        image : banzai.images.Image
                Image that should be linked

        Notes
        -----
        File name, site, instrument, dayobs and timestamp are always saved in the database.
        """

        es_output = {}
        if getattr(self.pipeline_context, 'post_to_elasticsearch', False):
            filename, results_to_save = format_qc_results(qc_results, image)
            es = elasticsearch.Elasticsearch(self.pipeline_context.elasticsearch_url)
            try:
                es_output = es.update(index=self.pipeline_context.elasticsearch_qc_index,
                                      doc_type=self.pipeline_context.elasticsearch_doc_type,
                                      id=filename, body={'doc': results_to_save, 'doc_as_upsert': True},
                                      retry_on_conflict=5, timestamp=results_to_save['@timestamp'], **kwargs)
            except Exception as e:
                error_message = 'Cannot update elasticsearch index to URL \"{url}\": {exception}'
                self.logger.error(error_message.format(url=self.pipeline_context.elasticsearch_url, exception=e))
        return es_output


class CalibrationMaker(Stage):
    def __init__(self, pipeline_context):
        super(CalibrationMaker, self).__init__(pipeline_context)

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    @abc.abstractmethod
    def make_master_calibration_frame(self, images, image_config, logging_tags):
        pass

    @property
    @abc.abstractmethod
    def min_images(self):
        return 5

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
    def __init__(self, pipeline_context):
        super(ApplyCalibration, self).__init__(pipeline_context)

    @property
    @abc.abstractmethod
    def calibration_type(self):
        pass

    def on_missing_master_calibration(self, logging_tags):
        self.logger.error('Master Calibration file does not exist for {stage}'.format(stage=self.stage_name),
                          extra=logging_tags)
        raise MasterCalibrationDoesNotExist

    def do_stage(self, images):
        if len(images) == 0:
            # Abort!
            return []
        else:
            image_config = image_utils.check_image_homogeneity(images)
            logging_tags = logs.image_config_to_tags(image_config, self.group_by_keywords)
            master_calibration_filename = self.get_calibration_filename(images[0])

            if master_calibration_filename is None:
                self.on_missing_master_calibration(logging_tags)

            master_calibration_image = Image(self.pipeline_context,
                                             filename=master_calibration_filename)
            return self.apply_master_calibration(images, master_calibration_image, logging_tags)

    @abc.abstractmethod
    def apply_master_calibration(self, images, master_calibration_image, logging_tags):
        pass

    def get_calibration_filename(self, image):
        return dbs.get_master_calibration_image(image, self.calibration_type, self.group_by_keywords,
                                                db_address=self.pipeline_context.db_address)
