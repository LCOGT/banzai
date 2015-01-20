import pylcogt
import numpy as np
import pyfits
import glob
from pylcogt.utils import pymysql
from pylcogt.utils.pymysql import readkey
import string
import lacosmicx
from sklearn.gaussian_process import GaussianProcess
import os
import shutil

def ingest(list_image, table, _force):
    conn = pymysql.getconnection()
    for img in list_image:
        img0 = string.split(img, '/')[-1]
        command = ['select filename from ' + str(table) + ' where filename="'
                   + str(img0) + '"']
        exist = pylcogt.query(command, conn)

        if not exist or _force in ['update', 'yes']:
            hdr = pyfits.getheader(img)
            _instrument = readkey(hdr, 'instrume')

            if _instrument in pymysql.instrument0['sinistro']:
                dir2 = 'preproc/'
            else:
                dir2 = 'raw/'

            if table in ['lcogtraw']:
                dire = pymysql.rawdata + readkey(hdr, 'SITEID') + '/' + readkey(hdr, 'instrume') + '/' + readkey(hdr, 'DAY-OBS') + '/' + dir2
            elif table in ['lcogtredu']:
                dire = pymysql.workingdirectory + readkey(hdr, 'SITEID') + '/' + readkey(hdr, 'instrume') + '/' + readkey(hdr, 'DAY-OBS') + '/'

            if _instrument in pymysql.instrument0['sbig'] + pymysql.instrument0['sinistro']:
                print '1m telescope'
                dictionary = {'dayobs': readkey(hdr, 'DAY-OBS'), 'exptime': readkey(hdr, 'exptime'),
                    'filter': readkey(hdr, 'filter'), 'mjd': readkey(hdr, 'MJD'),
                    'telescope': readkey(hdr, 'telescop'), 'airmass': readkey(hdr, 'airmass'),
                    'object': readkey(hdr, 'object'), 'ut': readkey(hdr, 'ut'),
                    'tracknum': readkey(hdr, 'TRACKNUM'), 'instrument': readkey(hdr, 'instrume'),
                    'ra0': readkey(hdr, 'RA'), 'dec0': readkey(hdr, 'DEC'), 'obstype': readkey(hdr, 'OBSTYPE'),
                    'reqnum': readkey(hdr, 'REQNUM'), 'groupid': readkey(hdr, 'GROUPID'),
                    'propid': readkey(hdr, 'PROPID'), 'userid': readkey(hdr, 'USERID'),
                    'dateobs': readkey(hdr, 'DATE-OBS'), 'ccdsum': readkey(hdr, 'CCDSUM')}
                dictionary['filename'] = string.split(img, '/')[-1]
                dictionary['filepath'] = dire
            elif _instrument in pymysql.instrument0['spectral']:
                print '2m telescope'
                dictionary = {'dayobs': readkey(hdr, 'DAY-OBS'), 'exptime': readkey(hdr, 'exptime'),
                    'filter': readkey(hdr, 'filter'), 'mjd': readkey(hdr, 'MJD'),
                    'telescope': readkey(hdr, 'telescop'), 'airmass': readkey(hdr, 'airmass'),
                    'object': readkey(hdr, 'object'), 'ut': readkey(hdr, 'ut'),
                    'tracknum': readkey(hdr, 'TRACKNUM'), 'instrument': readkey(hdr, 'instrume'),
                    'ra0': readkey(hdr, 'RA'), 'dec0': readkey(hdr, 'DEC'), 'obstype': readkey(hdr, 'OBSTYPE'),
                    'reqnum': readkey(hdr, 'REQNUM'), 'groupid': readkey(hdr, 'GROUPID'),
                    'propid': readkey(hdr, 'PROPID'), 'userid': readkey(hdr, 'USERID'),
                    'dateobs': readkey(hdr, 'DATE-OBS'), 'ccdsum': readkey(hdr, 'CCDSUM')}
                dictionary['filename'] = string.split(img, '/')[-1]
                dictionary['filepath'] = dire
            else:
                dictionary = ''
        else:
            print 'data already there'
            dictionary = ''

        if dictionary:
            if not exist:
                print 'insert values'
                pymysql.insert_values(conn, table, dictionary)
            else:
                print table
                print 'update values'
                for voce in dictionary:
                    print voce
                    # for voce in ['filepath','ra0','dec0','mjd','exptime','filter','ccdsum']:
                    for voce in ['ccdsum', 'filepath']:
                            pymysql.updatevalue(conn, table, voce, dictionary[voce],
                                                              string.split(img, '/')[-1], 'filename')
        else:
            print 'dictionary empty'

#################################################################################################################


def run_ingest(telescope, listepoch, _force, table='lcogtraw'):
    pymysql.site0
    if telescope == 'all':
        tellist = pymysql.site0
    elif telescope in pymysql.telescope0['elp'] + ['elp']:
        tellist = ['elp']
    elif telescope in pymysql.telescope0['lsc'] + ['lsc']:
        tellist = ['lsc']
    elif telescope in pymysql.telescope0['cpt'] + ['cpt']:
        tellist = ['cpt']
    elif telescope in pymysql.telescope0['coj'] + ['coj', 'fts']:
        tellist = ['coj']
    elif telescope in pymysql.telescope0['ogg'] + ['ftn']:
        tellist = ['ogg']

    if telescope in ['ftn', 'fts', '2m0-01', '2m0-02']:
        instrumentlist = pymysql.instrument0['spectral']
    else:
        instrumentlist = pymysql.instrument0['sinistro'] + pymysql.instrument0['sbig']

    for epoch in listepoch:
        for tel in tellist:
            for instrument in instrumentlist:
                if instrument in pymysql.instrument0['sinistro']:
                    dir2 = '/preproc/'
                else:
                    dir2 = '/raw/'
                imglist = glob.glob(pymysql.rawdata + tel + '/' + instrument + '/' + epoch + dir2 + '*')
                print imglist
                if len(imglist):
                    print 'ingest'
                    ingest(imglist, table, _force)

######################################################################################################################


def tofits(filename, data, hdr=None, clobber=False):
    """simple pyfits wrapper to make saving fits files easier."""
    from pyfits import PrimaryHDU, HDUList
    hdu = PrimaryHDU(data)
    if not (hdr is None):
        hdu.header += hdr
    hdulist = HDUList([hdu])
    hdulist.writeto(filename, clobber=clobber, output_verify='ignore')

#Combine images stored in a 3d array. Do a mean, rejecting pixels that are n MAD away from the median
def imcombine():
    return
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
        if np.std(imarr) / imagemad(imarr) > 10:
            good = False
    
        # If the diagonal part of the of the image covariance matrix is large
        # likely bad
        elif np.abs(imagecov(imarr)) ** 0.5 > 30:
            good = False
    
        # If the image std of the ratio of this flat and the previous flat is large
        # likely bad
        # This assumes the flats change slowly over time
        # If that is not the case, manual override is necessary
      #  elif np.std(imarr / imagemode(imarr, 200) / (goodarr / imagemode(goodarr, 200))) > 0.05:
        #     good = False
        goodflats[i] = good
    return goodflats

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
    flatdata = np.zeros((goodflats.sum(), ny, nx))

    for i, im in enumerate(imagenames[goodflats]):
        flatdata[i, :, :] = pyfits.getdata(im)[:, :]
        print("Finding the mode of flat field %s" % im)
        flatdata[i, :, :] /= imagemode(flatdata[i], 200)
    print("Median combining flat fields.")
    if len(imagenames) >= minimages:
        medflat = np.median(flatdata, axis=0)
        print("Finishing median combine.")
        hdr = pyfits.getheader(imagenames[0])
        hdr = sanitizeheader(hdr)
        tofits(outfilename, medflat, hdr=hdr, clobber=clobber)


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
        cmd = 'solve-field --crpix-center --no-tweak --no-verify --no-fits2fits'
        cmd += ' --radius 1.0 --ra %s --dec %s --guess-scale ' % (ra, dec)
        cmd += '--scale-units arcsecperpix --scale-low 0.1 --scale-high 1.0 '
        cmd += '--no-plots --use-sextractor -N tmpwcs.fits '
        if clobber: cmd += '--overwrite '
        cmd += '--solved none --match none --rdls none --wcs none --corr none '
        cmd += '%s' % im
        os.system(cmd)
        basename = im[:-5]
        os.remove(basename + '.axy')
        os.remove(basename + '-indx.xyls')
        shutil.move('tmpwcs.fits', outputnames[i])
