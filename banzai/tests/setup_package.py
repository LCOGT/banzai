from __future__ import absolute_import, division, print_function, unicode_literals
def get_package_data():
    return {
        _ASTROPY_PACKAGE_NAME_ + '.tests': ['coveragerc'],
        'package_data':'data/*'}
