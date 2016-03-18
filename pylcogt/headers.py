from pylcogt.stages import Stage
from pylcogt.utils import date_utils
from datetime import timedelta


class HeaderUpdater(Stage):
    def __init__(self, pipeline_context):
        super(HeaderUpdater, self).__init__(pipeline_context)

    @property
    def group_by_keywords(self):
        return None

    def do_stage(self, images):
        for image in images:
            image.header['RLEVEL'] = self.pipeline_context.rlevel

            if instantly_public(image.header['PROPID']):
                image.header['L1PUBDAT'] = image.header['DATE-OBS']
            else:
                # Wait a year
                date_observed = date_utils.parse_date_obs(image.header['DATE-OBS'])
                next_year = date_observed + timedelta(days=365)
                image.header['L1PUBDAT'] = date_utils.date_obs_to_string(next_year)
        return images


def instantly_public(proposal_id):
    public_now = False
    if proposal_id in ['calibrate', 'standard', 'pointing']:
        public_now = True
    if 'epo' in proposal_id.lower():
        public_now = True
    return public_now