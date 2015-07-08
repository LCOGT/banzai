from __future__ import absolute_import, print_function
__author__ = 'cmccully'

from astropy.io import fits
import numpy as np

def run_makebias(imagenames, outfilename, minimages=5, clobber=True):
    # Assume the files are all the same number of pixels, should add error checking
    nx = fits.getval(imagenames[0], ('NAXIS1'))
    ny = fits.getval(imagenames[0], ('NAXIS2'))
    biasdata = np.zeros((len(imagenames), ny, nx))

    if len(imagenames) >= minimages:
        for i, f in enumerate(imagenames):
            biasdata[i, :, :] = fits.getdata(f)[:, :]
            #Subtract the overscan

        medbias = np.median(biasdata, axis=0)
        hdr = sanitizeheader(pyfits.getheader(imagenames[0]))
        fits.writeto(outfilename, medbias, hdr=hdr, clobber=clobber)
        return outfilename
    else:
        return None
