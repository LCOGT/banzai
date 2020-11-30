import os
import numpy as np

from distutils.core import Extension
from extension_helpers import add_openmp_flags_if_available

UTIL_DIR = os.path.relpath(os.path.dirname(__file__))

def get_extensions():
    med_sources = [str(os.path.join(UTIL_DIR, "median_utils.pyx")),
                   str(os.path.join(UTIL_DIR, "quick_select.c"))]

    include_dirs = [np.get_include(), UTIL_DIR]
    libraries = []

    if 'CFLAGS' in os.environ:
        extra_compile_args = os.environ['CFLAGS'].split()
    else:
        extra_compile_args = ['-g', '-O3', '-funroll-loops', '-ffast-math']

    ext_med = Extension(name=str('banzai.utils.median_utils'),
                        sources=med_sources,
                        include_dirs=include_dirs,
                        libraries=libraries,
                        language="c",
                        extra_compile_args=extra_compile_args)

    add_openmp_flags_if_available(ext_med)

    return [ext_med]
