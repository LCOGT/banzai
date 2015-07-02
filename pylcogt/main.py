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
    parser.add_option("--instrument", dest="instrument", default='', type="str",
                      help='--instrument ' + ', '.join(pylcogt.utils.pymysql.instrument0['all']) +' \t [%default]')
    parser.add_option("-s", "--stage", dest="stage", default='', type="str",
                       help='-s stage [ingest,makebias, makeflat, makedark, applybias, applydark, hdupdate'
                            'applyflat, cosmic, wcs, checkimg] \t [%default]')
    parser.add_option("-f", "--filter", dest="filter", default='', type="str",
                      help="-f filter [sloan,landolt,apass,u,g,r,i,z,U,B,V,R,I] \t [%default]")
    parser.add_option("-b", "--bin", dest="bin", default='', type="str",
                      help="-b bin [1x1, 2x2 ] \t [%default]")
    parser.add_option("--table", dest="table", default='', type="str",
                      help="--table lcogtraw [lcogtraw,lcogtredu] \t [%default]")
    parser.add_option("--type", dest="type", default='', type="str",
                      help="--type SKYFLAT [EXPOSE,BIAS,DARK] \t [%default]")
    parser.add_option("-n", "--name", dest="name", default='', type="str",
                      help='-n image name   \t [%default]')
    parser.add_option("-d", "--id", dest="id", default='', type="str",
                      help='-d identification id   \t [%default]')

    option, args = parser.parse_args()
    # _instrument=option.instrument

    _telescope = option.telescope
    _instrument = option.instrument
    _table = option.table
    _stage = option.stage
    _type = option.type
    epoch = option.epoch
    _filter = option.filter
    _bin = option.bin
    _id = option.id
    _name = option.name

    if _telescope not in pylcogt.utils.pymysql.telescope0['all'] + pylcogt.utils.pymysql.site0 + \
            ['all', 'ftn', 'fts', '1m0', 'kb', 'fl', 'fs']:
            sys.argv.append('--help')

    if _table not in ['', 'lcogtraw', 'lcogtredu']:
            sys.argv.append('--help')

    if _stage not in ['makebias', 'makedark', 'makeflat', 'applybias', 'applydark', 'applyflat', 'wcs',
                      'cosmic','ingest','checkimg','hdupdate','']:
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
       pylcogt.utils.lcogtsteps.run_ingest(_telescope,_instrument,listepoch,'update','lcogtraw')
    else:
        if not _table:
            if _stage in ['makebias', 'applybias']:
                _table = 'lcogtraw'
            elif _stage in ['makedark','makeflat', 'applydark', 'applyflat', 'cosmic','wcs', 'checkimg','hdupdate']:
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

            if _filter or _bin or _name or _id:
                ll0 = pylcogt.utils.pymysql.filtralist(ll0, _filter, _id, _name, '', '', _bin)
            if not len(ll0['id']):
                sys.exit('no images selected')

            ###############################################################################3
            #  check which images are not processed
#
#            if _stage in ['applybias']:
#                if len(listepoch) == 1:
#                    listimgredu = pylcogt.utils.pymysql.getlistfromraw(conn, 'lcogtredu', 'dayobs', str(listepoch[0]), '', '*',
#                                                                       _telescope)
#                else:
#                    listimgredu = pylcogt.utils.pymysql.getlistfromraw(conn, 'lcogtredu', 'dayobs', str(listepoch[0]),
#                                                                       str(listepoch[-1]), '*', _telescope)
#                if len(listimgredu):
#                    ll1 = {}
#                    for jj in listimgredu[0].keys():
#                        ll1[jj] = []
#                    for i in range(0, len(listimgredu)):
#                        for jj in listimgredu[0].keys():
#                            ll1[jj].append(listimgredu[i][jj])
#                    ruthname = [i[:-9] for i in ll1['filename']]
#                    missing=[i for i in ll0['filename'] if i[:-9] not in ruthname]
#                    print 'missing data'
#                    for hh in missing:
#                        print hh
#                    print '\n'+'#'*30+'\n'
#                    if len(missing):
#                        ww=[i for i in range(0,len(ll0['filename'])) if ll0['filename'][i] in missing]
#                        for jj in ll0.keys():
#                            ll0[jj] = ll0[jj][ww]
#                        raw_input('stop here')
        #################################################################################

            for ind in range(0,len(ll0['filename'])): 
                print '%s\t%s\t%s\t%s\t%s\t%s\t' % \
                    (ll0['filename'][ind],ll0['dayobs'][ind],ll0['obstype'][ind],ll0['instrument'][ind],ll0['object'][ind],ll0['filter'][ind])
            print '#'*30
            print '\n number of images selected: ',len(ll0['filename']),'\n'
            print '#'*30
        else:
            sys.exit('no images selected')

        if _stage == 'makebias':
            print 'select bias and make bias'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['BIAS'])])
            if ww.size:
                listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
                listbin=ll0['ccdsum'][ww]
                listinst=ll0['instrument'][ww]
                listday=ll0['dayobs'][ww]
                for k in set(listinst):
                    for i in set(listbin):
                        for j in set(listday):
                            print k,j,i
                            listbias=np.array(listfile)[(listbin==i) & (listinst==k) & (listday==j)]
                            if listbias.size:
                                _output = 'bias_'+str(k)+'_'+re.sub('-','',str(j))+'_bin'+re.sub(' ','x',i)+'.fits'
                                _output = pylcogt.utils.lcogtsteps.run_makebias(listbias, _output, minimages=5)
                                if _output is not None:
                                    siteid=pyfits.getheader(_output)['SITEID']
                                    directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                                    if not os.path.isdir(directory): 
                                        os.mkdir(directory)
                                    print 'mv '+_output+' '+directory
                                    pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                                    os.system('mv '+_output+' '+directory)
                            else:
                                print 'no bias selected '+' '.join([str(k)+str(j)+str(i)])
            else:
                print 'no bias selected'

        elif _stage == 'makeflat':
            print 'select flat and make flat'
            #ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'])])
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'] and
                                                                        'SKYFLAT_bin' not in ll0['filename'][i])])
            if ww.size:
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
                                listflat=np.array(listfile)[(listbin == i) & (listday == j) & (listinst==k) & (listfilt == filt)]
                                if listflat.size:
                                    _output='flat_'+str(k)+'_'+re.sub('-','',str(j))+'_SKYFLAT_bin'+re.sub(' ','x',i)+'_'+str(filt)+'.fits'
                                    _output = pylcogt.utils.lcogtsteps.run_makeflat(listflat, _output, minimages=5)                            
                                    if _output is not None:
                                        print _output
                                        siteid=pyfits.getheader(_output)['SITEID']
                                        directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                                        if not os.path.isdir(directory): 
                                            os.mkdir(directory)
                                        print 'mv '+_output+' '+directory
                                        pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                                        os.system('mv '+_output+' '+directory)
                                    else:
                                        print 'no flat selected '+' '.join([str(k)+str(j)+str(i)])
            else:
                print 'no flat selected'

        elif _stage == 'makedark':
            print 'select dark and make dark'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['DARK'] and
                                                                            'dark_' not in ll0['filename'][i])])
            if ww.size:
                listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
                listbin=ll0['ccdsum'][ww]
                listinst=ll0['instrument'][ww]
                listday=ll0['dayobs'][ww]
                for k in set(listinst):
                    for i in set(listbin):
                        for j in set(listday):
                            print k,j,i
                            listdark=np.array(listfile)[(listbin == i) &  (listinst==k) & (listday == j)]
                            if listdark.size:
                                _output='dark_'+str(k)+'_'+re.sub('-','',str(j))+'_bin'+re.sub(' ','x',i)+'.fits'
                                _output = pylcogt.utils.lcogtsteps.run_makedark(listdark, _output, minimages=5)
                                if _output is not None:
                                    siteid=pyfits.getheader(_output)['SITEID']
                                    directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
                                    if not os.path.isdir(directory): 
                                        os.mkdir(directory)
                                    print 'mv '+_output+' '+directory
                                    pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
                                    os.system('mv '+_output+' '+directory)
                            else:
                                print 'no dark selected '+' '.join([str(k)+str(j)+str(i)])
            else:
                print 'no dark selected'

        elif _stage == 'applybias':
            print 'apply bias to science frame'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE',
                                                                                              'DARK','SKYFLAT'])])
            if ww.size:
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
                            #  filter also by instrument
                            listimg = np.array(listfile)[(listbin == i) & (listday == j) & (listinst == k)]
                            outfilenames = [re.sub('00.fits','90.fits',re.sub('02.fits','00.fits',string.split(ii,'/')[-1]))
                                        for ii in listimg]
                            listmjd0=np.array(listmjd)[(listbin == i) & (listday == j) & (listinst == k)]

                            #    select only images where bias was not apply
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
                                        if not os.path.isdir(directory): 
                                            os.mkdir(directory)
                                        print 'mv '+img+' '+directory
                                        pylcogt.utils.lcogtsteps.ingest([img], 'lcogtredu', 'no')
                                        os.system('mv '+img+' '+directory)
            else:
                print 'no exposure selected'

        elif _stage == 'applydark':
            print 'apply dark to science frame and flat'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE', 'SKYFLAT']
                                                                        and  'SKYFLAT' not in ll0['filename'][i])])
            if ww.size:
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
                            #  filter also by instrument
                            listimg = np.array(listfile)[(listbin == i) & (listday == j) & (listinst == k)]
                            listmjd0=np.array(listmjd)[(listbin == i) & (listday == j) & (listinst == k)]
                            outfilenames = [re.sub('00.fits','90.fits',string.split(ii,'/')[-1]) for ii in listimg]
                            #    select only images where Dark was not apply
                            jj = np.asarray([ii for ii in range(0,len(listimg))
                                             if not pyfits.getheader(listimg[ii]).get('DARKCOR')])
                            if len(jj):
                                listimg=np.array(listimg)[jj]
                                listmjd0=np.array(listmjd0)[jj]
                                outfilenames=np.array(outfilenames)[jj]
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
                                        if not os.path.isdir(directory): 
                                            os.mkdir(directory)
                                        print 'mv '+img+' '+directory
                                        pylcogt.utils.lcogtsteps.ingest([img], 'lcogtredu', 'no')
                                        os.system('mv '+img+' '+directory)
            else:
                print 'no exposures selected'

        elif _stage == 'applyflat':
            print 'apply flat to science frame'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            if ww.size:
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
                                listimg=np.array(listfile)[(listbin == i) &  (listinst==k) & (listday == j) & (listfilt == filt)]
                                listmjd0=np.array(listmjd)[(listbin == i) &  (listinst==k) & (listday == j) & (listfilt == filt)]

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
            else:
                print 'no exposures selected'

        elif _stage == 'cosmic':
            print 'select science images and correct for cosmic'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            if ww.size:
                listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]

                #    select only images where flat, bias and dark are applyed
                jj = np.asarray([ii for ii in range(0,len(listfile))
                             if (pyfits.getheader(listfile[ii]).get('BIASCOR')) and
                                 pyfits.getheader(listfile[ii]).get('DARKCOR') and
                                 pyfits.getheader(listfile[ii]).get('FLATCOR') ])
                listfile = np.array(listfile)[jj]
                outfilenames = [re.sub('.fits','.bpm.fits',ii)   for ii in listfile]
                pylcogt.utils.lcogtsteps.run_crreject(listfile, outfilenames, clobber=True)

        elif _stage == 'wcs':
            print 'select science image and do astrometry'
            ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
            if ww.size:
                listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]

                #    select only images where flat, bias and dark are applyed
                jj = np.asarray([ii for ii in range(0,len(listfile))
                             if (pyfits.getheader(listfile[ii]).get('BIASCOR')) and
                                 pyfits.getheader(listfile[ii]).get('DARKCOR') and
                                 pyfits.getheader(listfile[ii]).get('FLATCOR') ])
                listfile = np.array(listfile)[jj]
                #outfilenames = [re.sub('.fits','.bpm.fits',ii)   for ii in listfile]
                pylcogt.utils.lcogtsteps.run_astrometry(listfile, listfile, clobber=True)
                for im in listfile:
                    if pyfits.getheader(im).get('IMAGEH'):
                        pylcogt.utils.pymysql.updateheader(im,0, 
                                                           {'WCSERR':[0,' ASTROMETRY'],
                                                            'ASTROMET': ['1 1 1', 'rmsx rmsy nstars']
                                                            })
                    else:
                        pylcogt.utils.pymysql.updateheader(im,0, {'WCSERR':[1,' ASTROMETRY']
                                                                  })
            else:
                print 'no exposures selected'
        elif _stage == 'checkimg':
            if _type:
                ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in [_type])])
            else:
                ww = np.asarray([i for i in range(len(ll0['filename']))])
            if ww.size:
                listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
                from pyraf import iraf
                for img in listfile:
                    iraf.display(img,frame=1, fill='yes')
                    print img
                    answ = raw_input('good [y/n] [y] ')
                    if answ in ['n','No','NO']:
                        answ='n'
                        print 'delete image'
                        os.system('mv '+img+' '+'/nethome/supernova/pylcogt/bad/')
                        gg=pylcogt.utils.pymysql.getlistfromraw(conn, _table, 'filename', string.split(img,'/')[-1], 
                                                                '', column2='*', telescope='all')
                        if len(gg):
                            command = ['delete from lcogtredu where filename = "'+string.split(img,'/')[-1]+'"']
                            deletefromdb=pylcogt.utils.pymysql.query(command, conn)
                            print deletefromdb
        elif _stage == 'hdupdate':
            if _type:
                ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in [_type])])
            else:
                ww = np.asarray([i for i in range(len(ll0['filename']))])
            if ww.size:
                listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
                pylcogt.utils.lcogtsteps.run_hdupdate(listfile)
