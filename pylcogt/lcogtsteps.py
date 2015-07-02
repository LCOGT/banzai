from __future__ import absolute_import, print_function

import numpy as np
import glob
from . import dbs
import logging
import os
from astropy.io import fits
from astropy import time




def run_makebias(imagenames, outfilename, minimages=5, clobber=True):
    # Assume the files are all the same number of pixels, should add error checking
    nx = pyfits.getval(imagenames[0], ('NAXIS1'))
    ny = pyfits.getval(imagenames[0], ('NAXIS2'))
    biasdata = np.zeros((len(imagenames), ny, nx))
    for i, f in enumerate(imagenames):
        biasdata[i, :, :] = pyfits.getdata(f)[:, :]
        #Subtract the overscan
    if len(imagenames) >= minimages:
        medbias = np.median(biasdata, axis=0)
        hdr = sanitizeheader(pyfits.getheader(imagenames[0]))
        tofits(outfilename, medbias, hdr=hdr, clobber=clobber)
        return outfilename
    else:
        return None

#####################################################################################################################
def imagecov(imagearr):
    M00 = imagearr.sum()
    ny, nx = imagearr.shape
    x = np.linspace(1, nx, nx)
    y = np.linspace(1, ny, ny)
    # Grid the x and y values
    x2d, y2d = np.meshgrid(x, y)

    M10 = (x2d * imagearr).sum()
    M01 = (y2d * imagearr).sum()
    M20 = (x2d * x2d * imagearr).sum()
    M02 = (y2d * y2d * imagearr).sum()

    M11 = (x2d * y2d * imagearr).sum()

    xbar = M10 / M00
    ybar = M01 / M00

    mu20 = M20 - xbar * M10
    mu02 = M02 - ybar * M01
    mu11 = M11 - xbar * M01

    mu20 /= M00
    mu02 /= M00
    mu11 /= M00
    return (mu20, mu02, mu11)

def imagemode(imagearr, nbins):
    # Calculate the mode of an image
    minval = np.floor(np.min(imagearr))
    maxval = np.ceil(np.max(imagearr))
    # Pad the bins to be an integer value
    # nbins = int(maxval) - int(minval)
    hist = np.histogram(imagearr, nbins, (minval, maxval))
    binwidth = (maxval - minval) / nbins
    xmax = hist[1][np.argmax(hist[0])] + binwidth / 2.0

    # Get the optimal bin size from the Gaussian Kernel Density Estimator
    immad = np.median(np.abs(imagearr - np.median(imagearr)))
    finalbinwidth = 1.06 * (imagearr.shape[0] * imagearr.shape[1]) ** -0.2 * immad

    # Sanity check
    if finalbinwidth > 0.2 * binwidth:
        finalbinwidth = binwidth / 20.0
    finalrange = (xmax - 2.0 * binwidth, xmax + 2.0 * binwidth)
    finalnbins = (finalrange[1] - finalrange[0]) / finalbinwidth
    hist2 = np.histogram(imagearr, finalnbins, finalrange)

    y = hist2[0]
    w = y > 0
    y = y[w]
    X = hist2[1][:-1] + (hist2[1][1] - hist2[1][0]) / 2.0
    X = X[w]
    X = np.atleast_2d(X).T

    gp = GaussianProcess(corr='squared_exponential', theta0=1e-1,
                     thetaL=1e-3, thetaU=1,
                     nugget=1.0 / y,
                     random_start=10)
    # Fit to data using Maximum Likelihood Estimation of the parameters
    gp.fit(X, y)


    x = np.atleast_2d(np.linspace(min(X), max(X), 10 * nbins)).T
    y_pred = gp.predict(x)

    return x[np.argmax(y_pred)][0]

def imagemad(imagearr):
    immad = np.median(np.abs(imagearr - np.median(imagearr)))
    return immad

def goodflat(imagenames, previousgoodimagename):
    # goodarr = pyfits.getdata(previousgoodimagename)
    goodflats = np.zeros(len(imagenames), dtype=np.bool)
    for i, imagename in enumerate(imagenames):
        imarr = pyfits.getdata(imagename).copy()
    
        good = True
    
        # If more than 50% of the pixels are close to saturation it is bad
        saturate = float(pyfits.getval(imagename, 'SATURATE'))
        if saturate == 0.0: 
            saturate = 65536.
        if (imarr > (0.8 * saturate)).sum() > 0.5 * imarr.ravel().shape[0]:
            good = False
        # If the std and median absolute devition differ by a huge factor
        # likely bad
        #if np.std(imarr) / imagemad(imarr) > 10:
        #    good = False
    
        # If the diagonal part of the of the image covariance matrix is large
        # likely bad
        #elif np.abs(imagecov(imarr)[0]) ** 0.5 > 30:
        #    good = False
    
        # If the image std of the ratio of this flat and the previous flat is large
        # likely bad
        # This assumes the flats change slowly over time
        # If that is not the case, manual override is necessary
      #  elif np.std(imarr / imagemode(imarr, 200) / (goodarr / imagemode(goodarr, 200))) > 0.05:
        #     good = False
        goodflats[i] = good
    return goodflats


def run_applygain(imagenames, outputnames, clobber=True):
    for i, im in enumerate(images):
        hdu = pyfits.open(im)
        imdata *= float(hdu[0].header['GAIN'])
        hdu[0].header['GAIN'] = 1.0
        hdu.writeto(outputnames[i], clobber=clobber)
        
def run_subtractbias(imagenames, outfilenames, masterbiasname, clobber=True):
    # Assume the files are all the same number of pixels, should add error checking
    biashdu = pyfits.open(masterbiasname)
    biasdata = biashdu[0].data.copy()
    biashdu.close()

    for i, im in enumerate(imagenames):
        hdu = pyfits.open(im)
        imdata = hdu[0].data.copy()
        imhdr = sanitizeheader(hdu[0].header)
        hdu.close()
        #Subtract the overscan first if it exists
        imdata -= biasdata
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)

def sanitizeheader(hdr):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    hdr = hdr.copy()

    # Let the new data decide what these values should be
    for i in ['SIMPLE', 'BITPIX', 'BSCALE', 'BZERO']:
        if i in hdr.keys():
            hdr.pop(i)

#    if hdr.has_key('NAXIS'):
    if 'NAXIS' in hdr.keys():
        naxis = hdr.pop('NAXIS')
        for i in range(naxis):
            hdr.pop('NAXIS%i' % (i + 1))

    return hdr

def run_makedark(imagenames, outfilename, minimages=3, clobber=True):
    # Darks should already be bias subtracted

    # Load all of the images in normalizing by the mode of the pixel distribution
    # Assume all of the images have the same size,
    # FIXME Add error checking
    nx = pyfits.getval(imagenames[0], ('NAXIS1'))
    ny = pyfits.getval(imagenames[0], ('NAXIS2'))

    darkdata = np.zeros((len(imagenames), ny, nx))
    for i, im in enumerate(imagenames):
        darkdata[i, :, :] = pyfits.getdata(im)[:, :]
        darkdata[i, :, :] /= float(pyfits.getval(im, 'EXPTIME'))

    if len(imagenames) >= minimages:
        meddark = np.median(darkdata, axis=0)
        hdr = pyfits.getheader(imagenames[0])
        hdr = sanitizeheader(hdr)
        tofits(outfilename, meddark, hdr=hdr, clobber=clobber)
        return outfilename
    else:
        return None

def run_applydark(imagenames, outfilenames, masterdarkname, clobber=True):
    darkhdu = pyfits.open(masterdarkname)
    darkdata = darkhdu[0].data.copy()
    darkhdu.close()

    for i, im in enumerate(imagenames):
        hdu = pyfits.open(im)
        imdata = hdu[0].data.copy()
        imhdr = hdu[0].header.copy()
        imhdr = sanitizeheader(imhdr)
        exptime = float(imhdr['EXPTIME'])
        hdu.close()
        imdata -= darkdata * exptime
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)


def run_makeflat(imagenames, outfilename, minimages=3, clobber=True):
    # Flats should already be bias subtracted and dark corrected
    
    imagenames = np.array(imagenames)
    # Load all of the images in normalizing by the mode of the pixel distribution
    # Assume all of the images have the same size,
    # FIXME Add error checking
    nx = pyfits.getval(imagenames[0], ('NAXIS1'))
    ny = pyfits.getval(imagenames[0], ('NAXIS2'))
    
    goodflats = goodflat(imagenames, None)
    print goodflats
    if goodflats.sum() < minimages:
        print('No good flats')
        return None
    flatdata = np.zeros((goodflats.sum(), ny, nx))

    for i, im in enumerate(imagenames[goodflats]):
        flatdata[i, :, :] = pyfits.getdata(im)[:, :]
        print("Finding the mode of flat field %s" % im)
        flatdata[i, :, :] /= imagemode(flatdata[i], 200)
    print("Median combining flat fields.")
    medflat = np.median(flatdata, axis=0)
    print("Finishing median combine.")
    hdr = pyfits.getheader(imagenames[0])
    hdr = sanitizeheader(hdr)
    tofits(outfilename, medflat, hdr=hdr, clobber=clobber)
    return outfilename


def run_applyflat(imagenames, outfilenames, masterflatname, clobber=True):
    flathdu = pyfits.open(masterflatname)
    flatdata = flathdu[0].data.copy()
    flathdu.close()

    for i, im in enumerate(imagenames):
        hdu = pyfits.open(im)
        imdata = hdu[0].data.copy()
        imhdr = hdu[0].header.copy()
        imhdr = sanitizeheader(imhdr)
        hdu.close()
        imdata /= flatdata
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)


def run_crreject(imagenames, outputnames, clobber=True):
    # We can grab a master BPM here if we like
    for i, im in enumerate(imagenames):
        print 'cosmic rejection for image ' + str(im)
        hdu = pyfits.open(im)
        imdata = hdu[0].data.copy()
        imhdr = hdu[0].header.copy()
        imhdr = sanitizeheader(imhdr)
        hdu.close()
        gain = float(imhdr['GAIN'])
        saturate = float(imhdr['SATURATE'])
        rdnoise = float(imhdr['RDNOISE'])
        # Calculate observed noise level (median absolute deviation)
        mad = imagemad(imdata)
        med = np.median(imdata)  # Presumably the sky level
        # We expect that the sqrt(gain * median + RDNoise^2) should = MAD
        # Inverting this (MAD^2 - RDNOISE^2) / gain = median
        pssl = (mad * mad - rdnoise * rdnoise) / gain - med
        m = lacosmicx.lacosmicx(imdata, sigclip=4.5, sigfrac=0.3, objlim=5.0,
                                pssl=pssl, gain=gain, cleantype='idw',
                                satlevel=saturate, readnoise=rdnoise)
        tofits(outputnames[i], np.array(m, dtype=np.uint8), hdr=imhdr, clobber=clobber)


def run_astrometry(imagenames, outputnames, clobber=True):
    for i, im in enumerate(imagenames):
        print 'astrometry for image ' + str(im)
        # Run astrometry.net
        ra = pyfits.getval(im, 'RA')
        dec = pyfits.getval(im, 'DEC')
        cmd = 'solve-field --crpix-center --no-verify --no-fits2fits' #--no-tweak
        cmd += ' --radius 1.0 --ra %s --dec %s --guess-scale ' % (ra, dec)
        cmd += '--scale-units arcsecperpix --scale-low 0.1 --scale-high 1.0 '
        cmd += '--no-plots -N tmpwcs.fits '
        if clobber: cmd += '--overwrite '
        cmd += '--solved none --match none --rdls none --wcs none --corr none '
        cmd += ' --downsample 4 '
        cmd += '%s' % im
        os.system(cmd)
        basename = im[:-5]
        if os.path.exists(basename + '.axy'):
            os.remove(basename + '.axy')
        if os.path.exists(basename + '-indx.xyls'):
            os.remove(basename + '-indx.xyls')
        if os.path.exists('tmpwcs.fits'):
            #shutil.move('tmpwcs.fits', outputnames[i])
            hdrt = pyfits.getheader('tmpwcs.fits')
            dictionary = {
                'CTYPE1'  : [ 'RA---TAN', 'TAN (gnomic) projection'],
                'CTYPE2'  : ['DEC--TAN' , 'TAN (gnomic) projection'],
                'WCSAXES' : [ hdrt['WCSAXES'] , 'no comment'],
                'EQUINOX' : [ hdrt['EQUINOX'] , 'Equatorial coordinates definition (yr)'],
                'LONPOLE' : [ hdrt['LONPOLE'] , 'no comment'],
                'LATPOLE' : [ hdrt['LATPOLE'] , 'no comment'],
                'CRVAL1'  : [ hdrt['CRVAL1']  , 'RA  of reference point'],
                'CRVAL2'  : [ hdrt['CRVAL2'] , 'DEC of reference point'],
                'CRPIX1'  : [ hdrt['CRPIX1']     , 'X reference pixel'],
                'CRPIX2'  : [ hdrt['CRPIX2']     , 'Y reference pixel'],
                'CUNIT1'  : ['deg     ' , 'X pixel scale units'],
                'CUNIT2'  : ['deg     ' , 'Y pixel scale units'],
                'CD1_1'   : [ hdrt['CD1_1'] , 'Transformation matrix'],
                'CD1_2'   : [ hdrt['CD1_2'] , 'no comment'],
                'CD2_1'   : [ hdrt['CD2_1'] , 'no comment'],
                'CD2_2'   : [ hdrt['CD2_2'] , 'no comment'],
                'IMAGEW'  : [ hdrt['IMAGEW']  , 'Image width,  in pixels.'],
                'IMAGEH'  : [ hdrt['IMAGEH']  , 'Image height, in pixels.']}
            pylcogt.utils.pymysql.updateheader(im,0, dictionary)
            os.remove('tmpwcs.fits')

def run_sextractor(im):
        _sat = pyfits.getval(im, ('SATURATE'))
        if not _sat:
            _sat = 55000 
        line='NUMBER\nX_IMAGE\nY_IMAGE\nMAG_BEST\nMAGERR_BEST\nFLAGS\nCLASS_STAR\n'+\
            'FLUX_RADIUS\nELLIPTICITY\nBACKGROUND\nTHRESHOLD\n'
        f = open('default.param','w')
        f.write(line)
        f.close()

        line='CONV NORM\n# 3x3 ``all-ground'' convolution mask with FWHM = 2 pixels.\n'+\
            '1 2 1\n2 4 2\n1 2 1'
        f = open('default.conv','w')
        f.write(line)
        f.close()

        line='NNW\n# Neural Network Weights for the SExtractor star/galaxy classifier (V1.3)\n'+\
            '# inputs:	9 for profile parameters + 1 for seeing.\n# outputs:	``Stellarity index'' (0.0 to 1.0)\n'+\
            '# Seeing FWHM range: from 0.025 to 5.5'' (images must have 1.5 < FWHM < 5 pixels)\n'+\
            '# Optimized for Moffat profiles with 2<= beta <= 4.\n\n 3 10 10  1\n\n'+\
            '-1.56604e+00 -2.48265e+00 -1.44564e+00 -1.24675e+00 -9.44913e-01 -5.22453e-01  4.61342e-02  8.31957e-01  2.15505e+00  2.64769e-01\n'+\
            '3.03477e+00  2.69561e+00  3.16188e+00  3.34497e+00  3.51885e+00  3.65570e+00  3.74856e+00  3.84541e+00  4.22811e+00  3.27734e+00\n\n'+\
            '-3.22480e-01 -2.12804e+00  6.50750e-01 -1.11242e+00 -1.40683e+00 -1.55944e+00 -1.84558e+00 -1.18946e-01  5.52395e-01 -4.36564e-01 -5.30052e+00\n'+\
            ' 4.62594e-01 -3.29127e+00  1.10950e+00 -6.01857e-01  1.29492e-01  1.42290e+00  2.90741e+00  2.44058e+00 -9.19118e-01  8.42851e-01 -4.69824e+00\n'+\
            '-2.57424e+00  8.96469e-01  8.34775e-01  2.18845e+00  2.46526e+00  8.60878e-02 -6.88080e-01 -1.33623e-02  9.30403e-02  1.64942e+00 -1.01231e+00\n'+\
            ' 4.81041e+00  1.53747e+00 -1.12216e+00 -3.16008e+00 -1.67404e+00 -1.75767e+00 -1.29310e+00  5.59549e-01  8.08468e-01 -1.01592e-02 -7.54052e+00\n'+\
            ' 1.01933e+01 -2.09484e+01 -1.07426e+00  9.87912e-01  6.05210e-01 -6.04535e-02 -5.87826e-01 -7.94117e-01 -4.89190e-01 -8.12710e-02 -2.07067e+01\n'+\
            '-5.31793e+00  7.94240e+00 -4.64165e+00 -4.37436e+00 -1.55417e+00  7.54368e-01  1.09608e+00  1.45967e+00  1.62946e+00 -1.01301e+00  1.13514e-01\n'+\
            ' 2.20336e-01  1.70056e+00 -5.20105e-01 -4.28330e-01  1.57258e-03 -3.36502e-01 -8.18568e-02 -7.16163e+00  8.23195e+00 -1.71561e-02 -1.13749e+01\n'+\
            ' 3.75075e+00  7.25399e+00 -1.75325e+00 -2.68814e+00 -3.71128e+00 -4.62933e+00 -2.13747e+00 -1.89186e-01  1.29122e+00 -7.49380e-01  6.71712e-01\n'+\
            '-8.41923e-01  4.64997e+00  5.65808e-01 -3.08277e-01 -1.01687e+00  1.73127e-01 -8.92130e-01  1.89044e+00 -2.75543e-01 -7.72828e-01  5.36745e-01\n'+\
            '-3.65598e+00  7.56997e+00 -3.76373e+00 -1.74542e+00 -1.37540e-01 -5.55400e-01 -1.59195e-01  1.27910e-01  1.91906e+00  1.42119e+00 -4.35502e+00\n\n'+\
            '-1.70059e+00 -3.65695e+00  1.22367e+00 -5.74367e-01 -3.29571e+00  2.46316e+00  5.22353e+00  2.42038e+00  1.22919e+00 -9.22250e-01 -2.32028e+00\n\n'+\
            '0.00000e+00 \n 1.00000e+00 \n'
        f = open('default.nnw','w')
        f.write(line)
        f.close()

        os.system('sex -d > default.sex')
        line = 'sex '+im+' -CLEAN YES -PHOT_FLUXFRAC 0.5 -CATALOG_NAME detections.cat -SATUR_LEVEL '+str(_sat)+\
               ' -PIXEL_SCALE  .5 -SEEING_FWHM  2  -DETECT_MINAREA  10 -PARAMETERS_NAME default.param '+\
               ' > _logsex'
        os.system(line)
        data = np.genfromtxt('detections.cat')
        num,xpix,ypix,cm,cmerr,flag,cl,fw,ell,bkg,fl=zip(*data)
        return  num,xpix,ypix,cm,cmerr,flag,cl,fw,ell,bkg,fl

#########################################################################
def run_hdupdate(listfile):
    for im in listfile:
        _instrume =  pyfits.getheader(im)['instrume']
        if _instrume in pylcogt.instrument0['sbig']:
            fwscale = .68*2.35*0.467
            saturation = 46000
        elif _instrume in pylcogt.instrument0['sinistro']:
            fwscale = .68*2.35*0.467          #  need to check
            saturation = 99750
        elif _instrume in ['fs01','fs02','fs03']:
            fwscale = .68*2.35*0.30
            saturation = 60000
        elif _instrume in ['em03','em01']:
            fwscale = .68*2.35*0.278
            saturation = 60000
        else:
            fwscale = .68*2.35*0.467          #  need to check
            saturation = 46000

        num,xpix,ypix,cm,cmerr,flag,cl,fw,ell,bkg,fl = pylcogt.utils.lcogtsteps.run_sextractor(im)
        if len(fw)>1:
            fwhm = np.median(np.array(fw))*fwscale
        else:
            fwhm = 5
        
        pylcogt.utils.pymysql.updateheader(im,0, 
                                           {'SATURATE' : [saturation, '[ADU] Saturation level used'],
                                            'PSF_FWHM': [fwhm, 'FHWM (arcsec) - computed with sectractor'], 
                                            'L1FWHM': [fwhm, 'FHWM (arcsec) - computed with sectractor'], 
                                            })
#######################################################################
