# #!/usr/bin/env python
#
# # Licensed under a 3-clause BSD style license - see LICENSE.rst
#
# import builtins
#
# from setuptools import setup
# from setuptools.config import read_configuration
#
# from astropy_helpers.setup_helpers import register_commands, get_package_info
# from astropy_helpers.version_helpers import generate_version_py
#
# # Store the package name in a built-in variable so it's easy
# # to get from other parts of the setup infrastructure
# builtins._ASTROPY_PACKAGE_NAME_ = read_configuration('setup.cfg')['metadata']['name']
#
# # Create a dictionary with setup command overrides. Note that this gets
# # information about the package (name and version) from the setup.cfg file.
# cmdclass = register_commands()
#
# # Freeze build information in version.py. Note that this gets information
# # about the package (name and version) from the setup.cfg file.
# version = 1.0.6
#
# # Get configuration information from all of the various subpackages.
# # See the docstring for setup_helpers.update_package_files for more
# # details.
# package_info = get_package_info()
#
# setup(version=version, cmdclass=cmdclass, **package_info)

#!/usr/bin/env python
# Licensed under a 3-clause BSD style license - see LICENSE.rst

# NOTE: The configuration for the package, including the name, version, and
# other information are set in the setup.cfg file.

import os
import sys

from setuptools import setup

from extension_helpers import get_extensions
# First provide helpful messages if contributors try and run legacy commands
# for tests or docs.

TEST_HELP = """
Note: running tests is no longer done using 'python setup.py test'. Instead
you will need to run:

    tox -e test

If you don't already have tox installed, you can install it with:

    pip install tox

If you only want to run part of the test suite, you can also use pytest
directly with::

    pip install -e .[test]
    pytest

For more information, see:

  http://docs.astropy.org/en/latest/development/testguide.html#running-tests
"""

if 'test' in sys.argv:
    print(TEST_HELP)
    sys.exit(1)

DOCS_HELP = """
Note: building the documentation is no longer done using
'python setup.py build_docs'. Instead you will need to run:

    tox -e build_docs

If you don't already have tox installed, you can install it with:

    pip install tox

You can also build the documentation with Sphinx directly using::

    pip install -e .[docs]
    cd docs
    make html

For more information, see:

  http://docs.astropy.org/en/latest/install.html#builddocs
"""

if 'build_docs' in sys.argv or 'build_sphinx' in sys.argv:
    print(DOCS_HELP)
    sys.exit(1)

VERSION_TEMPLATE = """
# Note that we need to fall back to the hard-coded version if either
# setuptools_scm can't be imported or setuptools_scm can't determine the
# version, so we catch the generic 'Exception'.
try:
    from setuptools_scm import get_version
    version = get_version(root='..', relative_to=__file__)
except Exception:
    version = '{version}'
""".lstrip()

setup(use_scm_version={'write_to': os.path.join('banzai', 'version.py'),
                       'write_to_template': VERSION_TEMPLATE},
      ext_modules=get_extensions())
