from banzai.utils import file_utils

def test_get_basename_filename_fpacked():
    assert file_utils.get_basename('foo.fits.fz') == 'foo'

def test_get_basename_fits():
    assert file_utils.get_basename('foo.fits') == 'foo'

def test_get_basename_filepath():
    assert file_utils.get_basename('/foo/bar/baz.fits.fz') == 'baz'
