"""

@author: mnorbury
"""
import os

import numpy as np

import pytest

from astropy.io import fits

from banzai.main import *
from banzai.dbs import create_db


def setup_function(function):
    create_db('sqlite:///test.db')


def teardown_function(function):
    os.remove('test.db')


def full_pipeline():
    data_path = '/nethome/cmccully/workspace/banzai/banzai/tests/data/'
    site = 'elp'
    instrument = 'kb74'
    epoch = '20150325'
    final_image_name = 'elp1m008-kb74-20150325-0123-e90.fits'

    main('--db-host sqlite:///test.db --raw-path {0:s} --instrument {1:s} --processed-path {0:s} '\
         '--log-level debug --site {2:s} --epoch {3:s} '.format(data_path, instrument, site, epoch).split())

    expected_file_path = os.path.join(data_path, final_image_name)
    expected_hdu = fits.open(expected_file_path)

    actual_file_path = os.path.join(data_path, site, instrument, epoch, final_image_name)
    actual_hdu = fits.open(actual_file_path)

    assert expected_hdu[0].header == actual_hdu[0].header
    assert np.allclose(expected_hdu[0].data, actual_hdu[0].data, 1e-6)


if __name__ == '__main__':
    pytest.main([__file__, '--cov=banzai', '--cov-report=html', '--pdb'])
