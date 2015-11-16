"""

@author: mnorbury
"""
import os.path

import numpy as np

import pytest

from astropy.io import fits

from pylcogt.main import main
from pylcogt.dbs import create_db


def setup_function(function):
    create_db('sqlite:///test.db')


def teardown_function(function):
    pass


def test_something():
    root_output = '/home/mnorbury/tmp/'
    site = 'elp'
    instrument = 'kb74'
    epoch = '20150325'
    final_image_name = 'elp1m008-kb74-20150325-0123-e90.fits'

    main(
        '--raw-path /home/mnorbury/Pipeline/ --instrument {0:s} --processed-path {1:s} --log-level debug --site {2:s} --epoch {3:s}'.format(
            instrument, root_output,
            site,
            epoch).split())

    path, _ = os.path.split(__file__)
    expected_file_path = os.path.join(path, final_image_name)
    expected_hdu = fits.open(expected_file_path)

    actual_file_path = os.path.join(root_output, site, instrument, epoch, final_image_name)
    actual_hdu = fits.open(actual_file_path)

    assert expected_hdu[0].header == actual_hdu[0].header
    assert np.allclose(expected_hdu[0].data, actual_hdu[0].data, 1e-6)


if __name__ == '__main__':
    pytest.main([__file__, '--cov=pylcogt', '--cov-report=html'])
