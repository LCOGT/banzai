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
import numpy as np

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
        pylcogt.utils.lcogtsteps.run_ingest(_telescope,listepoch,'update')
    else:
        conn = pylcogt.utils.pymysql.getconnection()
        if len(listepoch) == 1:
            listimg = pylcogt.utils.pymysql.getlistfromraw(conn, 'lcogtraw', 'dayobs', str(listepoch[0]), '', '*',
                                                _telescope)
        else:
            listimg = pylcogt.utils.pymysql.getlistfromraw(conn, 'lcogtraw', 'dayobs', str(listepoch[0]),
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
                        print 'cp '+_output+' '+pylcogt.utils.pymysql.workingdirectory+listbias[0][0:3]+'/'+k+'/'
                        os.system('cp '+_output+' '+pylcogt.utils.pymysql.workingdirectory+listbias[0][0:3]+'/'+k+'/')
#                listfile[i]
        elif _stage == 'makeflat':
            print 'select flat and make flat'
        elif _stage == 'applybias':
            print 'apply bias to science frame'
        elif _stage == 'applyflat':
            print 'apply flat to science frame'
        elif _stage == 'cosmic':
            print 'select science images and correct for cosmic'
        elif _stage == 'wcs':
            print 'select science image and do astrometry'

