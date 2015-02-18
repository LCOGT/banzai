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
                      help='-T telescope ' + ', '.join(pylcogt.utils.pymysql.telescope0['all']) +' '+ ', '.join(
                          pylcogt.utils.pymysql.site0) + ', fts, ftn, 1m0, kb, fl \t [%default]')
    parser.add_option("-s", "--stage", dest="stage", default='', type="str",
                       help='-s stage [ingest,makebias, makeflat, makedark, applybias, applydark, '
                            'applyflat, cosmic, wcs] \t [%default]')
    parser.add_option("-f", "--filter", dest="filter", default='', type="str",
                      help="-f filter [sloan,landolt,apass,u,g,r,i,z,U,B,V,R,I] \t [%default]")
    parser.add_option("-b", "--bin", dest="bin", default='', type="str",
                      help="-b bin [1x1, 2x2 ] \t [%default]")

    option, args = parser.parse_args()
    # _instrument=option.instrument

    _telescope = option.telescope
    _stage = option.stage
    epoch = option.epoch
    _filter = option.filter
    _bin = option.bin

    if _telescope not in pylcogt.utils.pymysql.telescope0['all'] + pylcogt.utils.pymysql.site0 + \
            ['all', 'ftn', 'fts', '1m0', 'kb', 'fl', 'fs']:
            sys.argv.append('--help')

    if _stage not in ['makebias', 'makedark', 'makeflat', 'applybias', 'applydark', 'applyflat', 'wcs',
                      'cosmic','ingest','']:
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
        if _stage in ['makebias', 'applybias']:
            _table = 'lcogtraw'
        elif _stage in ['makedark','makeflat', 'applydark', 'applyflat', 'cosmic','wcs']:
            _table = 'lcogtredu'
        else:
            _table = 'lcogtraw'

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

            if _filter or _bin:
                ll0 = pylcogt.utils.pymysql.filtralist(ll0, _filter, '', '', '', '', _bin)
            if not len(ll0['id']):
                sys.exit('no images selected')
            print ll0['filename']
        else:
            sys.exit('no images selected')

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
                        listbias=np.array(listfile)[(listbin==i) & (listinst==k) & (listday==j)]
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
            #ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'])])
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'] and
                                                                        'SKYFLAT_bin' not in ll0['filename'][i])])
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
                            _output = pylcogt.utils.lcogtsteps.run_makeflat(listflat, _output, minimages=5)
                            
                            if not _output is None:
                                print _output
                                siteid=pyfits.getheader(_output)['SITEID']
                                directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                                print 'mv '+_output+' '+directory
                                pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                                os.system('mv '+_output+' '+directory)

        elif _stage == 'makedark':
            print 'select dark and make dark'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['DARK'] and
                                                                            'dark_' not in ll0['filename'][i])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
            listbin=ll0['ccdsum'][ww]
            listinst=ll0['instrument'][ww]
            listday=ll0['dayobs'][ww]
            for k in set(listinst):
                for i in set(listbin):
                    for j in set(listday):
                            print k,j,i
                            listdark=np.array(listfile)[(listbin == i) & (listday == j)]
                            _output='dark_'+str(k)+'_'+re.sub('-','',str(j))+'_bin'+re.sub(' ','x',i)+'.fits'
                            pylcogt.utils.lcogtsteps.run_makedark(listdark, _output, minimages=5)
                            print _output
                            siteid=pyfits.getheader(_output)['SITEID']
                            directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                            print 'mv '+_output+' '+directory
                            pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                            os.system('mv '+_output+' '+directory)

        elif _stage == 'applybias':
            print 'apply bias to science frame'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE',
                                                                                              'DARK','SKYFLAT'])])
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
                        outfilenames = [re.sub('00.fits','90.fits',re.sub('02.fits','00.fits',string.split(ii,'/')[-1]))
                                        for ii in listimg]
                        listmjd0=np.array(listmjd)[(listbin == i) & (listday == j)]

                        #    select only images where flat was not apply
                        jj = np.asarray([ii for ii in range(0,len(listimg))
                                             if not pyfits.getheader(listimg[ii]).get('BIASCOR')])
                        if len(jj):
                            listimg=np.array(listimg)[jj]
                            listmjd0=np.array(listmjd0)[jj]
                        else:
                            listimg=[]

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

        elif _stage == 'applydark':
            print 'apply dark to science frame and flat'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE', 'SKYFLAT']
                                                                        and  'SKYFLAT' not in ll0['filename'][i])])
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
                        outfilenames = [re.sub('00.fits','90.fits',string.split(ii,'/')[-1]) for ii in listimg]
                        listmjd0=np.array(listmjd)[(listbin == i) & (listday == j)]

                        #    select only images where flat was not apply
                        jj = np.asarray([ii for ii in range(0,len(listimg))
                                             if not pyfits.getheader(listimg[ii]).get('DARKCOR')])
                        if len(jj):
                                listimg=np.array(listimg)[jj]
                                listmjd0=np.array(listmjd0)[jj]
                        else:
                                listimg=[]


                        if len(listimg):
                            command=['select filepath,filename, mjd-'+str(listmjd0[0])+
                                     ' as diff from lcogtredu where ccdsum="'+str(i)+'" and instrument = "'+\
                                     str(k) +'" and obstype = "DARK" and filename like "%dark%" order by diff']
                            darkgood=pylcogt.utils.pymysql.query(command, conn)
                            if len(darkgood)>=1:
                                masterdarkname=darkgood[0]['filepath']+darkgood[0]['filename']
                                pylcogt.utils.lcogtsteps.run_applydark(listimg, outfilenames, masterdarkname, True)
                                for img in outfilenames:
                                    print img
                                    pylcogt.utils.pymysql.updateheader(img,0, {'DARKCOR':[string.split(masterdarkname,'/')[-1],' dark frame']})
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

                            #    select only images where flat was not apply
                            jj = np.asarray([ii for ii in range(0,len(listimg))
                                             if not pyfits.getheader(listimg[ii]).get('FLATCOR')])
                            if len(jj):
                                listimg=np.array(listimg)[jj]
                                listmjd0=np.array(listmjd0)[jj]
                            else:
                                listimg=[]

                            if len(listimg)>0:
                                command=['select filepath,filename, mjd-'+str(listmjd0[0])+' as diff from lcogtredu where ccdsum="'+\
                                         str(i)+'" and instrument = "'+\
                                         str(k)+'" and filter = "'+str(filt)+'" and obstype="SKYFLAT" and filename like "%flat%" order by diff']
                                flatgood=pylcogt.utils.pymysql.query(command, conn)
                                if len(flatgood) >= 1:
                                    masterflatname = flatgood[0]['filepath']+flatgood[0]['filename']
                                    print masterflatname
                                    print listimg
                                    print 'apply flat to science frame'
                                    pylcogt.utils.lcogtsteps.run_applyflat(listimg, listimg, masterflatname, True)
                                    for img in listimg:
                                        pylcogt.utils.pymysql.updateheader(img,0, {'FLATCOR':[string.split(masterflatname,'/')[-1],' flat frame']})
                                else:
                                    print 'no flat for this setup '+str(filt)+' '+str(i)

        elif _stage == 'cosmic':
            print 'select science images and correct for cosmic'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]

            #    select only images where flat, bias and dark are applyed
            jj = np.asarray([ii for ii in range(0,len(listfile))
                             if (pyfits.getheader(listfile[ii]).get('BIASCOR')) and
                                 pyfits.getheader(listfile[ii]).get('DARKCOR') and
                                 pyfits.getheader(listfile[ii]).get('FLATCOR') ])
            listfile = np.array(listfile)[jj]
            outfilenames = [re.sub('.fits','.bpm.fits',ii)   for ii in listfile]

            #raw_input('ddd')
            pylcogt.utils.lcogtsteps.run_crreject(listfile, outfilenames, clobber=True)

        elif _stage == 'wcs':
            print 'select science image and do astrometry'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]

            #    select only images where flat, bias and dark are applyed
            jj = np.asarray([ii for ii in range(0,len(listfile))
                             if (pyfits.getheader(listfile[ii]).get('BIASCOR')) and
                                 pyfits.getheader(listfile[ii]).get('DARKCOR') and
                                 pyfits.getheader(listfile[ii]).get('FLATCOR') ])
            listfile = np.array(listfile)[jj]
            #outfilenames = [re.sub('.fits','.bpm.fits',ii)   for ii in listfile]

            #raw_input('ddd')
            pylcogt.utils.lcogtsteps.run_astrometry(listfile, listfile, clobber=True)
            for im in listfile:
                if pyfits.getheader(im).get('IMAGEH'):
                    pylcogt.utils.pymysql.updateheader(im,0, {'WCSERR':[0,' ASTROMETRY']})
                else:
                    pylcogt.utils.pymysql.updateheader(im,0, {'WCSERR':[1,' ASTROMETRY']})
                    
