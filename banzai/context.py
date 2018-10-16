import sys
import logging
from lcogt_logging import LCOGTFormatter

from banzai import logs

logging.captureWarnings(True)

# Set up the root logger
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, 'DEBUG'))
root_handler = logging.StreamHandler(sys.stdout)

# Add handler
formatter = LCOGTFormatter()
root_handler.setFormatter(formatter)
root_logger.addHandler(root_handler)


class TelescopeCriterion:
    def __init__(self, attribute, comparison_operator, comparison_value, exclude=False):
        self.attribute = attribute
        self.comparison_value = comparison_value
        self.exclude = exclude
        self.comparison_operator = comparison_operator

    def telescope_passes(self, telescope):
        test = self.comparison_operator(getattr(telescope, self.attribute), self.comparison_value)
        if self.exclude:
            test = not test
        return test


class PipelineContext(object):
    def __init__(self, args, allowed_instrument_criteria):
        self.processed_path = args.processed_path
        self.raw_path = args.raw_path
        self.post_to_archive = args.post_to_archive
        self.post_to_elasticsearch = args.post_to_elasticsearch
        self.elasticsearch_url = args.elasticsearch_url
        self.fpack = args.fpack
        self.rlevel = args.rlevel
        self.db_address = args.db_address
        self.log_level = args.log_level
        self.preview_mode = args.preview_mode
        self.filename = args.filename
        self.max_preview_tries = args.max_preview_tries
        self.elasticsearch_doc_type = args.elasticsearch_doc_type
        self.elasticsearch_qc_index = args.elasticsearch_qc_index
        self.no_bpm = args.no_bpm
        self.allowed_instrument_criteria = allowed_instrument_criteria

        logs.set_log_level(self.log_level)
