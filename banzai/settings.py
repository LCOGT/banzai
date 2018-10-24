import sys
import logging

from lcogt_logging import LCOGTFormatter

logging.captureWarnings(True)

# Set up the root logger
root_logger = logging.getLogger()
root_handler = logging.StreamHandler(sys.stdout)

# Add handler
formatter = LCOGTFormatter()
root_handler.setFormatter(formatter)
root_handler.setLevel(getattr(logging, 'DEBUG'))
root_logger.addHandler(root_handler)
