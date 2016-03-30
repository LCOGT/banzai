import pylcogt
from pylcogt import logs
from pylcogt.stages import Stage
from pylcogt.utils import date_utils
from datetime import timedelta
import os


class HeaderUpdater(Stage):
    def __init__(self, pipeline_context):
        super(HeaderUpdater, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            image.header['RLEVEL'] = (self.pipeline_context.rlevel, 'Reduction level')
            image.header['PIPEVER'] = (pylcogt.__version__, 'Pipeline version')

            if instantly_public(image.header['PROPID']):
                image.header['L1PUBDAT'] = (image.header['DATE-OBS'],
                                            '[UTC] Date the frame becomes public')
            else:
                # Wait a year
                date_observed = date_utils.parse_date_obs(image.header['DATE-OBS'])
                next_year = date_observed + timedelta(days=365)
                image.header['L1PUBDAT'] = (date_utils.date_obs_to_string(next_year),
                                            '[UTC] Date the frame becomes public')
            logging_tags = logs.image_config_to_tags(image, self.group_by_keywords)
            logs.add_tag(logging_tags, 'filename', os.path.basename(image.filename))
            logs.add_tag(logging_tags, 'rlevel', int(image.header['RLEVEL']))
            logs.add_tag(logging_tags, 'pipeline_version', image.header['PIPEVER'])
            logs.add_tag(logging_tags, 'l1pubdat', image.header['L1PUBDAT'])
            self.logger.info('Updating header', extra=logging_tags)

        return images


def instantly_public(proposal_id):
    public_now = False
    if proposal_id in ['calibrate', 'standard', 'pointing']:
        public_now = True
    if 'epo' in proposal_id.lower():
        public_now = True
    return public_now
