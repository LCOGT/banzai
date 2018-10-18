import os
from glob import glob
import logging

from astropy.io import fits

from banzai import logs, dbs

logger = logging.getLogger(__name__)


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

    def __eq__(self, other):
        return self.__dict__ == other.__dict__


class PipelineContext(object):
    def __init__(self, command_line_args, allowed_instrument_criteria, processed_path='/archive/engineering/',
                 post_to_archive=False, fpack=True, rlevel=91, db_address='mysql://cmccully:password@localhost/test',
                 log_level='INFO', preview_mode=False, max_tries=5, post_to_elasticsearch=False,
                 elasticsearch_url='http://elasticsearch.lco.gtn:9200', elasticsearch_doc_type='qc',
                 elasticsearch_qc_index='banzai_qc', **kwargs):
        # TODO: preview_mode will be removed once we start processing everything in real time.
        # TODO: no_bpm can also be removed once we are in "do our best" mode
        local_variables = locals()
        for variable in local_variables:
            if variable == 'kwargs':
                kwarg_variables = local_variables[variable]
                for kwarg in kwarg_variables:
                    super(PipelineContext, self).__setattr__(kwarg, kwarg_variables[kwarg])
            elif variable != 'command_line_args':
                super(PipelineContext, self).__setattr__(variable, local_variables[variable])

        for keyword in vars(command_line_args):
            super(PipelineContext, self).__setattr__(keyword, getattr(command_line_args, keyword))

    def __delattr__(self, item):
        raise TypeError('Deleting attribute is not allowed. PipelineContext is immutable')

    def __setattr__(self, key, value):
        raise TypeError('Resetting attribute is not allowed. PipelineContext is immutable.')
        logs.set_log_level(self.log_level)

    def set_image_to_reduce(self, image_path):
        self.filename = os.path.basename(image_path)
        self.raw_path = os.path.dirname(image_path)

    def get_image_path(self):
        return os.path.join(self.raw_path, self.filename)

    def get_image_path_list(self):
        search_path = os.path.join(self.raw_path)

        # return the list of file and a dummy image configuration
        fits_files = glob(search_path + '/*.fits')
        fz_files = glob(search_path + '/*.fits.fz')

        fz_files_to_remove = []
        for i, f in enumerate(fz_files):
            if f[:-3] in fits_files:
                fz_files_to_remove.append(f)

        for f in fz_files_to_remove:
            fz_files.remove(f)
        return fits_files + fz_files

    def image_passes_criteria(self):
        telescope = dbs.get_telescope_for_file(self.get_image_path(), db_address=self.db_address)
        passes = True
        for criterion in self.allowed_instrument_criteria:
            if not criterion.telescope_passes(telescope):
                passes = False
        return passes

    def image_has_valid_obstype(self, image_types):
        obstype = None
        hdu_list = fits.open(self.get_image_path())
        for hdu in hdu_list:
            if 'OBSTYPE' in hdu.header.keys():
                obstype = hdu.header['OBSTYPE']
        if obstype is None:
            logger.error('Unable to get OBSTYPE', extra_tags={'filename': self.get_image_path()})
        return obstype in image_types

    def image_should_be_reduced(self, image_types):
        reduce_image = False
        try:
            if self.image_has_valid_obstype(image_types) and self.image_passes_criteria():
                reduce_image = True
        except Exception as e:
            logger.error('Exception checking image selection criteria: {e}'.format(e=e),
                         extra_tags={'filename': self.get_image_path()})
        return reduce_image
