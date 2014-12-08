import pylcogt
from astropy import units as u
import ccdproc

def ingest(list_image,table,_force):
    import pyfits
    from pylcogt.utils.pymysql import readkey
    from pylcogt.utils.pymysql import getconnection
    import string

    conn=getconnection()
    for img in list_image:
        img0 = string.split(img,'/')[-1]
        command=['select filename from '+str(table)+' where filename="'+str(img0)+'"']
        exist = pylcogt.query(command,conn)

        if not exist or _force in ['update','yes']:
            hdr = pyfits.getheader(img)
            _instrument = readkey(hdr, 'instrume')
            if _instrument in pylcogt.utils.pymysql.instrument0['sbig'] + pylcogt.utils.pymysql.instrument0['sinistro']:
                print '1m telescope'
                dictionary = {'dayobs': readkey(hdr, 'DAY-OBS'), 'exptime': readkey(hdr, 'exptime'),
                    'filter': readkey(hdr, 'filter'), 'mjd': readkey(hdr, 'MJD'),
                    'telescope': readkey(hdr, 'telescop'), 'airmass': readkey(hdr, 'airmass'),
                    'object': readkey(hdr, 'object'), 'ut': readkey(hdr, 'ut'),
                    'tracknum': readkey(hdr, 'TRACKNUM'), 'instrument': readkey(hdr, 'instrume'),
                    'ra0': readkey(hdr, 'RA'), 'dec0': readkey(hdr, 'DEC'),'obstype': readkey(hdr, 'OBSTYPE'),
                    'reqnum': readkey(hdr, 'REQNUM'),'groupid': readkey(hdr, 'GROUPID'),
                    'propid': readkey(hdr, 'PROPID'), 'userid': readkey(hdr, 'USERID'),
                    'dateobs': readkey(hdr, 'DATE-OBS')}
                dictionary['filename'] = string.split(img, '/')[-1]
                dictionary['filepath'] = pylcogt.utils.pymysql.workingdirectory + '1mtel/' + \
                                     readkey(hdr, 'DAY-OBS') + '/'
            elif _instrument in pylcogt.utils.pymysql.instrument0['spectral']:
                print '2m telescope'
                dictionary = {'dayobs': readkey(hdr, 'DAY-OBS'), 'exptime': readkey(hdr, 'exptime'),
                    'filter': readkey(hdr, 'filter'), 'mjd': readkey(hdr, 'MJD'),
                    'telescope': readkey(hdr, 'telescop'), 'airmass': readkey(hdr, 'airmass'),
                    'object': readkey(hdr, 'object'), 'ut': readkey(hdr, 'ut'),
                    'tracknum': readkey(hdr, 'TRACKNUM'), 'instrument': readkey(hdr, 'instrume'),
                    'ra0': readkey(hdr, 'RA'), 'dec0': readkey(hdr, 'DEC'),'obstype': readkey(hdr, 'OBSTYPE'),
                    'reqnum': readkey(hdr, 'REQNUM'),'groupid': readkey(hdr, 'GROUPID'),
                    'propid': readkey(hdr, 'PROPID'), 'userid': readkey(hdr, 'USERID'),
                    'dateobs': readkey(hdr, 'DATE-OBS')}
                dictionary['namefile'] = string.split(img, '/')[-1]
                dictionary['filepath'] = pylcogt.utils.pymysql.workingdirectory + '1mtel/' + \
                                    readkey(hdr, 'DAY-OBS') + '/'
            else:
                dictionary = ''
        else:
            print 'data already there'
            dictionary = ''

        if dictionary:
            if not exist:
                print 'insert values'
                pylcogt.utils.pymysql.insert_values(conn,table,dictionary)
            else:
                print table
                print 'update values'
                for voce in dictionary:
                    print voce
                    for voce in ['filepath','ra0','dec0','mjd','exptime','filter']:
                            pylcogt.utils.pymysql.updatevalue(conn,table,voce,dictionary[voce],
                                                              string.split(img,'/')[-1],'filename')
        else:
            print 'dictionary empty'

#################################################################################################################

def run_ingest(telescope,listepoch):
    import glob
    import pylcogt
    import string

    pylcogt.utils.pymysql.site0
    if telescope=='all':
        tellist=pylcogt.utils.pymysql.site0
    elif telescope in pylcogt.utils.pymysql.telescope0['elp']+['elp']:
        tellist=['elp']
    elif telescope in pylcogt.utils.pymysql.telescope0['lsc']+['lsc']:
        tellist=['lsc']
    elif telescope in pylcogt.utils.pymysql.telescope0['cpt']+['cpt']:
        tellist=['cpt']
    elif telescope in pylcogt.utils.pymysql.telescope0['coj']+['coj','fts']:
        tellist=['coj']
    elif telescope in pylcogt.utils.pymysql.telescope0['ogg']+['ftn']:
        tellist=['ogg']


    if telescope in ['ftn','fts','2m0-01','2m0-02']:
        instrumentlist = pylcogt.utils.pymysql.instrument0['spectral']
    else:
        instrumentlist = pylcogt.utils.pymysql.instrument0['sinistro']+pylcogt.utils.pymysql.instrument0['sbig']

    for epoch in listepoch:
        for tel in tellist:
            for instrument in instrumentlist:
                imglist = glob.glob(pylcogt.utils.pymysql.rawdata+tel+'/'+instrument+'/'+epoch+'/raw/*')
                print imglist
                if len(imglist):
                    print 'ingest'
                    ingest(imglist,'lcogtraw','no')

######################################################################################################################


def run_makebias(imagenames, outfilename, minimages=5):
    biasims = []
    for f in imagenames:
        biasims.append(ccdproc.CCDData.read(f, unit=u.adu))
    if len(biasims) >= minimages:
        biascombiner = ccdproc.Combiner(biasims)
        d = biascombiner.median_combine()
        ccdproc.CCDData.write(d, outfilename)


def run_subtractbias(imagenames, outfilenames, masterbiasname, clobber=False):
    ims = []
    for f in imagenames:
        ims.append(ccdproc.CCDData.read(f, unit=u.adu))
    masterbias = ccdproc.CCDData.read(masterbiasname, unit=u.adu)
    for i, im in enumerate(ims):
        d = ccdproc.subtract_bias(im, masterbias)
        ccdproc.CCDData.write(d, outfilenames[i], clobber=clobber)

