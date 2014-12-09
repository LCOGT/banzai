import pylcogt
import numpy as np
import numpy.ma as ma
import pyfits
import glob
import string
import ccdproc

def ingest(list_image, table, _force):
    import pyfits
    from pylcogt.utils.pymysql import readkey
    from pylcogt.utils.pymysql import getconnection
    import string

    conn = getconnection()
    for img in list_image:
        img0 = string.split(img, '/')[-1]
        command = ['select filename from ' + str(table) + ' where filename="' + str(img0) + '"']
        exist = pylcogt.query(command, conn)

        if not exist or _force in ['update', 'yes']:
            hdr = pyfits.getheader(img)
            _instrument = readkey(hdr, 'instrume')
            if table in ['lcogtraw']:
                dire = pylcogt.utils.pymysql.rawdata + readkey(hdr,'SITEID') + '/' + readkey(hdr, 'instrume')+ '/' + readkey(hdr, 'DAY-OBS') + '/raw/'
            elif table in ['lcogtredu']:
                dire = pylcogt.utils.pymysql.workingdirectory + readkey(hdr,'SITEID') + '/' + readkey(hdr, 'instrume')+ '/' + readkey(hdr, 'DAY-OBS') + '/'

            if _instrument in pylcogt.utils.pymysql.instrument0['sbig'] + pylcogt.utils.pymysql.instrument0['sinistro']:
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

            elif _instrument in pylcogt.utils.pymysql.instrument0['spectral']:
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
                pylcogt.utils.pymysql.insert_values(conn, table, dictionary)
            else:
                print table
                print 'update values'
                for voce in dictionary:
                    print voce
                    # for voce in ['filepath','ra0','dec0','mjd','exptime','filter','ccdsum']:
                    for voce in ['ccdsum', 'filepath']:
                            pylcogt.utils.pymysql.updatevalue(conn, table, voce, dictionary[voce],
                                                              string.split(img, '/')[-1], 'filename')
        else:
            print 'dictionary empty'

#################################################################################################################



def run_ingest(telescope,listepoch,_force,table='lcogtraw'):
    pylcogt.utils.pymysql.site0
    if telescope == 'all':
        tellist = pylcogt.utils.pymysql.site0
    elif telescope in pylcogt.utils.pymysql.telescope0['elp'] + ['elp']:
        tellist = ['elp']
    elif telescope in pylcogt.utils.pymysql.telescope0['lsc'] + ['lsc']:
        tellist = ['lsc']
    elif telescope in pylcogt.utils.pymysql.telescope0['cpt'] + ['cpt']:
        tellist = ['cpt']
    elif telescope in pylcogt.utils.pymysql.telescope0['coj'] + ['coj', 'fts']:
        tellist = ['coj']
    elif telescope in pylcogt.utils.pymysql.telescope0['ogg'] + ['ftn']:
        tellist = ['ogg']


    if telescope in ['ftn', 'fts', '2m0-01', '2m0-02']:
        instrumentlist = pylcogt.utils.pymysql.instrument0['spectral']
    else:
        instrumentlist = pylcogt.utils.pymysql.instrument0['sinistro'] + pylcogt.utils.pymysql.instrument0['sbig']

    for epoch in listepoch:
        for tel in tellist:
            for instrument in instrumentlist:
                imglist = glob.glob(pylcogt.utils.pymysql.rawdata + tel + '/' + instrument + '/' + epoch + '/raw/*')
                print imglist
                if len(imglist):
                    print 'ingest'
                    ingest(imglist,table,_force)

######################################################################################################################

def tofits(filename, data, hdr=None, clobber=False):
    """simple pyfits wrapper to make saving fits files easier."""
    from pyfits import PrimaryHDU, HDUList
    hdu = PrimaryHDU(data)
    if not hdr is None:
        hdu.header = hdr
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
        tofits(outfilename, medbias, hdr=pyfits.getheader(imagenames[0]), clobber=clobber)

#####################################################################################################################


def flatfieldmode(imagearr):
    # Note that currently we are just bias subtracting and since we are
    # median combining the bias frames, pixels can only have integer values or 0.5
    hist = np.histogram(imagearr,
                        np.round(np.max(imagearr) - np.min(imagearr)) * 2 + 1,
                        (np.min(imagearr) - 0.25, np.max(imagearr) + 0.25))
    m = np.argmax(hist[0])
    return hist[1][m] + 0.25


def run_subtractbias(imagenames, outfilenames, masterbiasname, clobber=True):
    # Assume the files are all the same number of pixels, should add error checking
    biashdu = pyfits.open(masterbiasname)
    biasdata = biashdu[0].data.copy()
    biashdu.close()

    for i, im in enumerate(imagenames):
        hdu = pyfits.open(im)
        imdata = hdu[0].data.copy()
        imhdr = hdu[0].header.copy()
        hdu.close()
        imdata -= biasdata
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)


def run_makeflat(imagenames, outfilename, minimages=3, clobber=True):
    # Flats should already be bias subtracted

    # Load all of the images in normalizing by the mode of the pixel distribution
    # Assume all of the images have the same size,
    # FIXME Add error checking
    nx = pyfits.getval(imagenames[0], ('NAXIS1'))
    ny = pyfits.getval(imagenames[0], ('NAXIS2'))

    flatdata = np.zeros((len(imagenames), ny, nx))
    for i, im in enumerate(imagenames):
        flatdata[i, :, :] = pyfits.getdata(im)[:, :]
        flatdata[i, :, :] /= flatfieldmode(flatdata[i, :, :])
    if len(imagenames) >= minimages:
        medflat = np.median(flatdata, axis=0)
        tofits(outfilename, medflat, hdr=pyfits.getheader(imagenames[0]),
               clobber=clobber)


def run_flatten(imagenames, outfilenames, masterflatname, clobber=True):
    flathdu = pyfits.open(masterflatname)
    flatdata = flathdu[0].data.copy()
    flathdu.close()

    for i, im in enumerate(imagenames):
        hdu = pyfits.open(im)
        imdata = hdu[0].data.copy()
        imhdr = hdu[0].header.copy()
        hdu.close()
        imdata /= flatdata
        tofits(outfilenames[i], imdata, hdr=imhdr, clobber=clobber)

