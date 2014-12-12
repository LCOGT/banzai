import pylcogt
import numpy as np
import pyfits
import glob
from pylcogt.utils import pymysql
from pylcogt.utils.pymysql import readkey
import string
from sklearn.gaussian_process import GaussianProcess

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
            if table in ['lcogtraw']:
                dire = pymysql.rawdata + readkey(hdr,'SITEID') + '/' + readkey(hdr, 'instrume')+ '/' + readkey(hdr, 'DAY-OBS') + '/raw/'
            elif table in ['lcogtredu']:
                dire = pymysql.workingdirectory + readkey(hdr,'SITEID') + '/' + readkey(hdr, 'instrume')+ '/' + readkey(hdr, 'DAY-OBS') + '/'

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
                dictionary['namefile'] = string.split(img, '/')[-1]
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


def run_ingest(telescope,listepoch,_force,table='lcogtraw'):
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
                imglist = glob.glob(pymysql.rawdata + tel + '/' + instrument + '/' + epoch + '/raw/*')
                print imglist
                if len(imglist):
                    print 'ingest'
                    ingest(imglist,table,_force)

######################################################################################################################


def tofits(filename, data, hdr=None, clobber=False):
    """simple pyfits wrapper to make saving fits files easier."""
    from pyfits import PrimaryHDU, HDUList
    hdu = PrimaryHDU(data)
    if not (hdr is None):
        hdu.header += hdr
    hdulist = HDUList([hdu])
    hdulist.writeto(filename, clobber=clobber, output_verify='ignore')


def run_makebias(imagenames, outfilename, minimages=5, clobber=True):
    # Assume the files are all the same number of pixels, should add error checking
    nx = pyfits.getval(imagenames[0], ('NAXIS1'))
    ny = pyfits.getval(imagenames[0], ('NAXIS2'))
    biasdata = np.zeros((len(imagenames), ny, nx))
    for i, f in enumerate(imagenames):
        biasdata[i, :, :] = pyfits.getdata(f)[:, :]
    if len(imagenames) >= minimages:
        medbias = np.median(biasdata, axis=0)
        hdr = sanitizeheader(pyfits.getheader(imagenames[0]))
        tofits(outfilename, medbias, hdr=hdr, clobber=clobber)

#####################################################################################################################


def imagemode(imagearr,nbins):
    #Calculate the mode of an image
    minval = np.floor(np.min(imagearr))
    maxval = np.ceil(np.max(imagearr))
    #Pad the bins to be an integer value
    #nbins = int(maxval) - int(minval)
    hist = np.histogram(imagearr, nbins, (minval, maxval))
    binwidth = (maxval -minval)/nbins
    xmax = hist[1][np.argmax(hist[0])] + binwidth/2.0

    # Get the optimal bin size from the Gaussian Kernel Density Estimator
    imstd = np.std(imagearr)
    finalbinwidth = 1.06 * (imagearr.shape[0]* imagearr.shape[1])**-0.2 * imstd
    
    finalrange = (xmax - 2.0*binwidth, xmax + 2.0*binwidth)
    finalnbins = (finalrange[1] - finalrange[0])/finalbinwidth
    hist2 = np.histogram(imagearr, finalnbins, finalrange)

    y = hist2[0]
    w = y > 0
    y = y[w]
    X = hist2[1][:-1] + (hist2[1][1] - hist2[1][0])/2.0
    X = X[w]
    X = np.atleast_2d(X).T

    gp = GaussianProcess(corr='squared_exponential', theta0=1e-1,
                     thetaL=1e-3, thetaU=1,
                     nugget=1.0/y,
                     random_start=10)
    # Fit to data using Maximum Likelihood Estimation of the parameters
    gp.fit(X, y)
    

    x = np.atleast_2d(np.linspace(min(X),max(X), 10*nbins)).T    
    y_pred = gp.predict(x)

    return x[np.argmax(y_pred)][0]


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
        imdata -= biasdata
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)

def sanitizeheader(hdr):
    # Remove the mandatory keywords from a header so it can be copied to a new
    # image.
    hdr = hdr.copy()

    # Let the new data decide what these values should be
    for i in ['SIMPLE', 'BITPIX', 'BSCALE','BZERO']:
        if hdr.has_key(i):
            hdr.pop(i)

    if hdr.has_key('NAXIS'):
        naxis = hdr.pop('NAXIS')
        for i in range(naxis):
            hdr.pop('NAXIS%i'%(i+1))
        
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
        darkdata[i, :, :] /= float(pyfits.getval(im,'EXPTIME'))

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

    # Load all of the images in normalizing by the mode of the pixel distribution
    # Assume all of the images have the same size,
    # FIXME Add error checking
    nx = pyfits.getval(imagenames[0], ('NAXIS1'))
    ny = pyfits.getval(imagenames[0], ('NAXIS2'))

    flatdata = np.zeros((len(imagenames), ny, nx))
    for i, im in enumerate(imagenames):
        flatdata[i, :, :] = pyfits.getdata(im)[:, :]
        flatdata[i, :, :] /= imagemode(flatdata[i],200)

    if len(imagenames) >= minimages:
        medflat = np.median(flatdata, axis=0)
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
        imhdr =sanitizeheader(imhdr)
        hdu.close()
        imdata /= flatdata
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)

