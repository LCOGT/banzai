#!/usr/bin/env python
#
description="> main pylcogt program to call all the steps "
usage= "%prog  -e epoch [-s stage -n name -f filter -d idnumber ]\n " \
       "available stages [makebias, makeflat, applybias, applyflat, cosmic, wcs]\n "
import sys
import pylcogt
import datetime
import string
import re
import os
import numpy as np
import pyfits
from optparse import OptionParser

if __name__ == "__main__":
    parser = OptionParser(usage=usage,description=description, version="%prog 1.0")
    parser.add_option("-e", "--epoch",dest="epoch",default='20121212',type="str",
                  help='epoch to reduce  \t [%default]')
    parser.add_option("-T", "--telescope", dest="telescope", default='all', type="str",
                      help='-T telescope ' + ', '.join(pylcogt.utils.pymysql.telescope0['all']) + ', '.join(
                          pylcogt.utils.pymysql.site0) + ', fts, ftn, 1m0, kb, fl \t [%default]')
    parser.add_option("-s", "--stage", dest="stage", default='', type="str",
                       help='-s stage [ingest,makebias, makeflat, applybias, applyflat, cosmic, wcs] \t [%default]')

    option, args = parser.parse_args()
    # _instrument=option.instrument

    _telescope = option.telescope
    _stage = option.stage
    epoch = option.epoch

    if _telescope not in pylcogt.utils.pymysql.telescope0['all'] + pylcogt.utils.pymysql.site0 + \
            ['all', 'ftn', 'fts', '1m0', 'kb', 'fl']:
            sys.argv.append('--help')

    if _stage not in ['makebias', 'makeflat', 'applybias', 'applyflat', 'wcs', 'cosmic','ingest','']:
            sys.argv.append('--help')

    option, args = parser.parse_args()

    if '-' not in str(epoch):
        epoch0 = datetime.date(int(epoch[0:4]), int(epoch[4:6]), int(epoch[6:8]))
        listepoch = [re.sub('-', '', str(epoch0))]
    else:
        epoch1, epoch2 = string.split(epoch, '-')
        start = datetime.date(int(epoch1[0:4]), int(epoch1[4:6]), int(epoch1[6:8]))
        stop = datetime.date(int(epoch2[0:4]), int(epoch2[4:6]), int(epoch2[6:8]))
        listepoch = [re.sub('-', '', str(i)) for i in [start + datetime.timedelta(days=x)
                                                       for x in range(0, 1 + (stop - start).days)]]

    print _telescope
    print _stage
    print listepoch
    if _stage == 'ingest':
        pylcogt.utils.lcogtsteps.run_ingest(_telescope,listepoch,'update','lcogtraw')
    else:
        if _stage in ['makebias', 'makeflat', 'applybias']:
            _table = 'lcogtraw'
        else:
            _table = 'lcogtredu'

        conn = pylcogt.utils.pymysql.getconnection()
        if len(listepoch) == 1:
            listimg = pylcogt.utils.pymysql.getlistfromraw(conn, _table, 'dayobs', str(listepoch[0]), '', '*',
                                                _telescope)
        else:
            listimg = pylcogt.utils.pymysql.getlistfromraw(conn, _table, 'dayobs', str(listepoch[0]),
                                                str(listepoch[-1]), '*', _telescope)

        if listimg:
            ll0 = {}
            for jj in listimg[0].keys():
                ll0[jj] = []
            for i in range(0, len(listimg)):
                for jj in listimg[0].keys():
                    ll0[jj].append(listimg[i][jj])
            inds = np.argsort(ll0['mjd'])  # sort by mjd
            for i in ll0.keys():
                ll0[i] = np.take(ll0[i], inds)
            print ll0['filename']

        if _stage == 'makebias':
            print 'select bias and make bias'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['BIAS'])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
            listbin=ll0['ccdsum'][ww]
            listinst=ll0['instrument'][ww]
            listday=ll0['dayobs'][ww]
            for k in set(listinst):
                for i in set(listbin):
                    for j in set(listday):
                        print k,j,i
                        listbias=np.array(listfile)[(listbin==i) & (listday==j)]
                        _output='bias_'+str(k)+'_'+re.sub('-','',str(j))+'_bin'+re.sub(' ','x',i)+'.fits'
                        pylcogt.utils.lcogtsteps.run_makebias(listbias, _output, minimages=5)
                        print _output
                        siteid=pyfits.getheader(_output)['SITEID']
                        directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                        if not os.path.isdir(directory): 
                            os.mkdir(directory)
                        print 'mv '+_output+' '+directory
                        pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                        os.system('mv '+_output+' '+directory)


        elif _stage == 'makeflat':
            print 'select flat and make flat'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
            listbin=ll0['ccdsum'][ww]
            listinst=ll0['instrument'][ww]
            listday=ll0['dayobs'][ww]
            listfilt=ll0['filter'][ww]
            for filt in set(listfilt):
                for k in set(listinst):
                    for i in set(listbin):
                        for j in set(listday):
                            print k,j,i
                            listflat=np.array(listfile)[(listbin == i) & (listday == j) & (listfilt == filt)]
                            _output='flat_'+str(k)+'_'+re.sub('-','',str(j))+'_SKYFLAT_bin'+re.sub(' ','x',i)+'_'+str(filt)+'.fits'
                            pylcogt.utils.lcogtsteps.run_makeflat(listflat, _output, minimages=5)
                            print _output
                            siteid=pyfits.getheader(_output)['SITEID']
                            directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                            print 'mv '+_output+' '+directory
                            pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                            os.system('mv '+_output+' '+directory)


        elif _stage == 'applybias':
            print 'apply bias to science frame'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
            listbin=ll0['ccdsum'][ww]
            listinst=ll0['instrument'][ww]
            listday=ll0['dayobs'][ww]
            listmjd=ll0['mjd'][ww]
            listfilt=ll0['filter'][ww]
            for k in set(listinst):
                for i in set(listbin):
                    for j in set(listday):
                        print j,k,i
                        listimg = np.array(listfile)[(listbin == i) & (listday == j)]
                        outfilenames = [re.sub('e00.fits','e90.fits',string.split(ii,'/')[-1]) for ii in listimg]
                        listmjd0=np.array(listmjd)[(listbin == i) & (listday == j)]
                        if len(listimg):
                            command=['select filepath,filename, mjd-'+str(listmjd0[0])+
                                     ' as diff from lcogtredu where ccdsum="'+str(i)+'" and instrument = "'+\
                                     str(k) +'" and obstype = "BIAS" order by diff']
                            biasgood=pylcogt.utils.pymysql.query(command, conn)
                            if len(biasgood)>=1:
                                masterbiasname=biasgood[0]['filepath']+biasgood[0]['filename']
                                pylcogt.utils.lcogtsteps.run_subtractbias(listimg, outfilenames, masterbiasname, True)
                                for img in outfilenames:
                                    print img
                                    pylcogt.utils.pymysql.updateheader(img,0, {'BIASCOR':[string.split(masterbiasname,'/')[-1],' bias frame']})
                                    siteid=pyfits.getheader(img)['SITEID']
                                    directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                                    print 'mv '+img+' '+directory
                                    pylcogt.utils.lcogtsteps.ingest([img], 'lcogtredu', 'no')
                                    os.system('mv '+img+' '+directory)


        elif _stage == 'applyflat':
            print 'apply flat to science frame'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
            listbin=ll0['ccdsum'][ww]
            listinst=ll0['instrument'][ww]
            listday=ll0['dayobs'][ww]
            listmjd=ll0['mjd'][ww]
            listfilt=ll0['filter'][ww]
            for filt in set(listfilt):
                for k in set(listinst):
                    for i in set(listbin):
                        for j in set(listday):
                            print j,k,i,filt
                            listimg=np.array(listfile)[(listbin == i) & (listday == j) & (listfilt == filt)]
                            listmjd0=np.array(listmjd)[(listbin == i) & (listday == j) & (listfilt == filt)]
                            if len(listimg)>0:
                                command=['select filepath,filename, mjd-'+str(listmjd0[0])+' as diff from lcogtredu where ccdsum="'+\
                                         str(i)+'" and instrument = "'+\
                                         str(k)+'" and filter = "'+str(filt)+'" and obstype="SKYFLAT" order by diff']
                                flatgood=pylcogt.utils.pymysql.query(command, conn)
                                if len(flatgood) >= 1:
                                    masterflatname = flatgood[0]['filepath']+flatgood[0]['filename']
                                    print masterflatname
                                    print listimg
                                    print 'apply flat to science frame'
                                    pylcogt.utils.lcogtsteps.run_flatten(listimg, listimg, masterflatname, True)
                                    for img in listimg:
                                        pylcogt.utils.pymysql.updateheader(img,0, {'FLATCOR':[string.split(masterflatname,'/')[-1],' flat frame']})

        elif _stage == 'cosmic':
            print 'select science images and correct for cosmic'
        elif _stage == 'wcs':
            print 'select science image and do astrometry'

