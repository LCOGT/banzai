
import argparse

import sqlalchemy
from .dateutils import parse_epoch_string
from .ingest import run_ingest
from . import dbs
from . import logs

reduction_stages = ['ingest', 'make_bias', 'make_flat', 'make_dark', 'apply_bias', 'apply_dark', 'apply_flat',
                    'cr_reject', 'wcs', 'check_image', 'hdu_update']

def get_telescope_info():
   # Get All of the telescope information
    db_session = dbs.get_session()
    all_sites = []
    for site in db_session.query(dbs.Telescope.site).distinct():
        all_sites.append(site[0])

    all_instruments = []
    for instrument in db_session.query(dbs.Telescope.instrument).distinct():
        all_instruments.append(instrument[0])

    all_telescope_ids = []
    for telescope_id in db_session.query(dbs.Telescope.telescope_id).distinct():
        all_telescope_ids.append(telescope_id[0])

    all_camera_types = []
    for camera_type in db_session.query(dbs.Telescope.camera_type).distinct():
        all_camera_types.append(camera_type[0])

    db_session.close()
    return all_sites, all_instruments, all_telescope_ids, all_camera_types


def main():
    # Get the telescope info
    all_sites, all_instruments, all_telescope_ids, all_camera_types = get_telescope_info()

    parser = argparse.ArgumentParser(description='Reduce LCOGT imaging data.')
    parser.add_argument("--epoch", required=True, type=str, help='Epoch to reduce')
    parser.add_argument("--telescope", default='', choices=all_telescope_ids,
                        help='Telescope ID (e.g. 1m0-010).')
    parser.add_argument("--instrument", default='', type=str, choices=all_instruments,
                        help='Instrument code (e.g. kb74)')
    parser.add_argument("--site", default='', type=str, choices=all_sites,
                        help='Site code (e.g. elp)')
    parser.add_argument("--camera_type", default='', type=str, choices=all_camera_types,
                        help='Camera type (e.g. sbig)')

    parser.add_argument("--stage", default='all', choices=['all'] + reduction_stages,
                        help='Reduction stages to run')

    parser.add_argument("--rawpath", default='/archive/engineering',
                        help='Top level directory where the raw data is stored')
    parser.add_argument("--processedpath", default='/nethome/supernova/pylcogt',
                        help='Top level directory where the processed data will be stored')


    # parser.add_argument("-f", "--filter", default='all', type="str",
    #                   help="-f filter [sloan,landolt,apass,u,g,r,i,z,U,B,V,R,I] \t [%default]")
    # parser.add_argument("-b", "--bin", dest="bin", default='', type="str",
    #                   help="-b bin [1x1, 2x2 ] \t [%default]")
    # parser.add_argument("--type", dest="type", default='', type="str",
    #                   help="--type SKYFLAT [EXPOSE,BIAS,DARK] \t [%default]")
    # parser.add_argument("-n", "--name", dest="name", default='', type="str",
    #                   help='-n image name   \t [%default]')
    # parser.add_argument("-d", "--id", dest="id", default='', type="str",
    #                   help='-d identification id   \t [%default]')

    args = parser.parse_args()

    logs.start_logging(filename='pylcogt.log')
    epoch_list = parse_epoch_string(args.epoch)

    stages_to_do = reduction_stages

    # Get the telescopes for which we want to reduce data.
    db_session = dbs.get_session()

    telescope_query = sqlalchemy.sql.expression.true()

    if args.site != '':
        telescope_query = (dbs.Telescope.site == args.site) & (telescope_query)

    if args.instrument != '':
        telescope_query = (dbs.Telescope.instrument == args.instrument) & (telescope_query)

    if args.telescope != '':
        telescope_query = (dbs.Telescope.telescope_id == args.telescope) & (telescope_query)

    if args.camera_type != '':
        telescope_query = (dbs.Telescope.camera_type == args.camera_type) & (telescope_query)

    telescope_list = db_session.query(dbs.Telescope).filter(telescope_query).all()

    db_session.close()
    if 'ingest' in stages_to_do:
        for telescope in telescope_list:
            run_ingest(args.rawpath, telescope.site, telescope.instrument,
                       epoch_list, args.processedpath)



#
#     else:
#
#         conn = pylcogt.utils.pymysql.getconnection()
#         if len(listepoch) == 1:
#             listimg = pylcogt.utils.pymysql.getlistfromraw(conn, _table, 'dayobs', str(listepoch[0]), '', '*',
#                                                 _telescope)
#         else:
#             listimg = pylcogt.utils.pymysql.getlistfromraw(conn, _table, 'dayobs', str(listepoch[0]),
#                                                 str(listepoch[-1]), '*', _telescope)
#
#         if listimg:
#             ll0 = {}
#             for jj in listimg[0].keys():
#                 ll0[jj] = []
#             for i in range(0, len(listimg)):
#                 for jj in listimg[0].keys():
#                     ll0[jj].append(listimg[i][jj])
#             inds = np.argsort(ll0['mjd'])  # sort by mjd
#             for i in ll0.keys():
#                 ll0[i] = np.take(ll0[i], inds)
#
#             if _filter or _bin or _name or _id:
#                 ll0 = pylcogt.utils.pymysql.filtralist(ll0, _filter, _id, _name, '', '', _bin)
#             if not len(ll0['id']):
#                 sys.exit('no images selected')
#
#             ###############################################################################3
#             #  check which images are not processed
# #
# #            if _stage in ['applybias']:
# #                if len(listepoch) == 1:
# #                    listimgredu = pylcogt.utils.pymysql.getlistfromraw(conn, 'lcogtredu', 'dayobs', str(listepoch[0]), '', '*',
# #                                                                       _telescope)
# #                else:
# #                    listimgredu = pylcogt.utils.pymysql.getlistfromraw(conn, 'lcogtredu', 'dayobs', str(listepoch[0]),
# #                                                                       str(listepoch[-1]), '*', _telescope)
# #                if len(listimgredu):
# #                    ll1 = {}
# #                    for jj in listimgredu[0].keys():
# #                        ll1[jj] = []
# #                    for i in range(0, len(listimgredu)):
# #                        for jj in listimgredu[0].keys():
# #                            ll1[jj].append(listimgredu[i][jj])
# #                    ruthname = [i[:-9] for i in ll1['filename']]
# #                    missing=[i for i in ll0['filename'] if i[:-9] not in ruthname]
# #                    print 'missing data'
# #                    for hh in missing:
# #                        print hh
# #                    print '\n'+'#'*30+'\n'
# #                    if len(missing):
# #                        ww=[i for i in range(0,len(ll0['filename'])) if ll0['filename'][i] in missing]
# #                        for jj in ll0.keys():
# #                            ll0[jj] = ll0[jj][ww]
# #                        raw_input('stop here')
#         #################################################################################
#
#             for ind in range(0,len(ll0['filename'])):
#                 print '%s\t%s\t%s\t%s\t%s\t%s\t' % \
#                     (ll0['filename'][ind],ll0['dayobs'][ind],ll0['obstype'][ind],ll0['instrument'][ind],ll0['object'][ind],ll0['filter'][ind])
#             print '#'*30
#             print '\n number of images selected: ',len(ll0['filename']),'\n'
#             print '#'*30
#         else:
#             sys.exit('no images selected')
#
#         if _stage == 'makebias':
#             print 'select bias and make bias'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['BIAS'])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 listbin=ll0['ccdsum'][ww]
#                 listinst=ll0['instrument'][ww]
#                 listday=ll0['dayobs'][ww]
#                 for k in set(listinst):
#                     for i in set(listbin):
#                         for j in set(listday):
#                             print k,j,i
#                             listbias=np.array(listfile)[(listbin==i) & (listinst==k) & (listday==j)]
#                             if listbias.size:
#                                 _output = 'bias_'+str(k)+'_'+re.sub('-','',str(j))+'_bin'+re.sub(' ','x',i)+'.fits'
#                                 _output = pylcogt.utils.lcogtsteps.run_makebias(listbias, _output, minimages=5)
#                                 if _output is not None:
#                                     siteid=pyfits.getheader(_output)['SITEID']
#                                     directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
#                                     if not os.path.isdir(directory):
#                                         os.mkdir(directory)
#                                     print 'mv '+_output+' '+directory
#                                     pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
#                                     os.system('mv '+_output+' '+directory)
#                             else:
#                                 print 'no bias selected '+' '.join([str(k)+str(j)+str(i)])
#             else:
#                 print 'no bias selected'
#
#         elif _stage == 'makeflat':
#             print 'select flat and make flat'
#             #ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'])])
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['SKYFLAT'] and
#                                                                         'SKYFLAT_bin' not in ll0['filename'][i])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 listbin=ll0['ccdsum'][ww]
#                 listinst=ll0['instrument'][ww]
#                 listday=ll0['dayobs'][ww]
#                 listfilt=ll0['filter'][ww]
#                 for filt in set(listfilt):
#                     for k in set(listinst):
#                         for i in set(listbin):
#                             for j in set(listday):
#                                 print k,j,i
#                                 listflat=np.array(listfile)[(listbin == i) & (listday == j) & (listinst==k) & (listfilt == filt)]
#                                 if listflat.size:
#                                     _output='flat_'+str(k)+'_'+re.sub('-','',str(j))+'_SKYFLAT_bin'+re.sub(' ','x',i)+'_'+str(filt)+'.fits'
#                                     _output = pylcogt.utils.lcogtsteps.run_makeflat(listflat, _output, minimages=5)
#                                     if _output is not None:
#                                         print _output
#                                         siteid=pyfits.getheader(_output)['SITEID']
#                                         directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
#                                         if not os.path.isdir(directory):
#                                             os.mkdir(directory)
#                                         print 'mv '+_output+' '+directory
#                                         pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
#                                         os.system('mv '+_output+' '+directory)
#                                     else:
#                                         print 'no flat selected '+' '.join([str(k)+str(j)+str(i)])
#             else:
#                 print 'no flat selected'
#
#         elif _stage == 'makedark':
#             print 'select dark and make dark'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['DARK'] and
#                                                                             'dark_' not in ll0['filename'][i])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 listbin=ll0['ccdsum'][ww]
#                 listinst=ll0['instrument'][ww]
#                 listday=ll0['dayobs'][ww]
#                 for k in set(listinst):
#                     for i in set(listbin):
#                         for j in set(listday):
#                             print k,j,i
#                             listdark=np.array(listfile)[(listbin == i) &  (listinst==k) & (listday == j)]
#                             if listdark.size:
#                                 _output='dark_'+str(k)+'_'+re.sub('-','',str(j))+'_bin'+re.sub(' ','x',i)+'.fits'
#                                 _output = pylcogt.utils.lcogtsteps.run_makedark(listdark, _output, minimages=5)
#                                 if _output is not None:
#                                     siteid=pyfits.getheader(_output)['SITEID']
#                                     directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
#                                     if not os.path.isdir(directory):
#                                         os.mkdir(directory)
#                                     print 'mv '+_output+' '+directory
#                                     pylcogt.utils.lcogtsteps.ingest([_output], 'lcogtredu', 'yes')
#                                     os.system('mv '+_output+' '+directory)
#                             else:
#                                 print 'no dark selected '+' '.join([str(k)+str(j)+str(i)])
#             else:
#                 print 'no dark selected'
#
#         elif _stage == 'applybias':
#             print 'apply bias to science frame'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE',
#                                                                                               'DARK','SKYFLAT'])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 listbin=ll0['ccdsum'][ww]
#                 listinst=ll0['instrument'][ww]
#                 listday=ll0['dayobs'][ww]
#                 listmjd=ll0['mjd'][ww]
#                 listfilt=ll0['filter'][ww]
#                 for k in set(listinst):
#                     for i in set(listbin):
#                         for j in set(listday):
#                             print j,k,i
#                             #  filter also by instrument
#                             listimg = np.array(listfile)[(listbin == i) & (listday == j) & (listinst == k)]
#                             outfilenames = [re.sub('00.fits','90.fits',re.sub('02.fits','00.fits',string.split(ii,'/')[-1]))
#                                         for ii in listimg]
#                             listmjd0=np.array(listmjd)[(listbin == i) & (listday == j) & (listinst == k)]
#
#                             #    select only images where bias was not apply
#                             jj = np.asarray([ii for ii in range(0,len(listimg))
#                                              if not pyfits.getheader(listimg[ii]).get('BIASCOR')])
#                             if len(jj):
#                                 listimg=np.array(listimg)[jj]
#                                 listmjd0=np.array(listmjd0)[jj]
#                             else:
#                                 listimg=[]
#
#                             if len(listimg):
#                                 command=['select filepath,filename, mjd-'+str(listmjd0[0])+
#                                      ' as diff from lcogtredu where ccdsum="'+str(i)+'" and instrument = "'+\
#                                      str(k) +'" and obstype = "BIAS" order by diff']
#                                 biasgood=pylcogt.utils.pymysql.query(command, conn)
#                                 if len(biasgood)>=1:
#                                     masterbiasname=biasgood[0]['filepath']+biasgood[0]['filename']
#                                     pylcogt.utils.lcogtsteps.run_subtractbias(listimg, outfilenames, masterbiasname, True)
#                                     for img in outfilenames:
#                                         print img
#                                         pylcogt.utils.pymysql.updateheader(img,0, {'BIASCOR':[string.split(masterbiasname,'/')[-1],' bias frame']})
#                                         siteid=pyfits.getheader(img)['SITEID']
#                                         directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
#                                         if not os.path.isdir(directory):
#                                             os.mkdir(directory)
#                                         print 'mv '+img+' '+directory
#                                         pylcogt.utils.lcogtsteps.ingest([img], 'lcogtredu', 'no')
#                                         os.system('mv '+img+' '+directory)
#             else:
#                 print 'no exposure selected'
#
#         elif _stage == 'applydark':
#             print 'apply dark to science frame and flat'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE', 'SKYFLAT']
#                                                                         and  'SKYFLAT' not in ll0['filename'][i])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 listbin=ll0['ccdsum'][ww]
#                 listinst=ll0['instrument'][ww]
#                 listday=ll0['dayobs'][ww]
#                 listmjd=ll0['mjd'][ww]
#                 listfilt=ll0['filter'][ww]
#                 for k in set(listinst):
#                     for i in set(listbin):
#                         for j in set(listday):
#                             print j,k,i
#                             #  filter also by instrument
#                             listimg = np.array(listfile)[(listbin == i) & (listday == j) & (listinst == k)]
#                             listmjd0=np.array(listmjd)[(listbin == i) & (listday == j) & (listinst == k)]
#                             outfilenames = [re.sub('00.fits','90.fits',string.split(ii,'/')[-1]) for ii in listimg]
#                             #    select only images where Dark was not apply
#                             jj = np.asarray([ii for ii in range(0,len(listimg))
#                                              if not pyfits.getheader(listimg[ii]).get('DARKCOR')])
#                             if len(jj):
#                                 listimg=np.array(listimg)[jj]
#                                 listmjd0=np.array(listmjd0)[jj]
#                                 outfilenames=np.array(outfilenames)[jj]
#                             else:
#                                 listimg=[]
#
#                             if len(listimg):
#                                 command=['select filepath,filename, mjd-'+str(listmjd0[0])+
#                                      ' as diff from lcogtredu where ccdsum="'+str(i)+'" and instrument = "'+\
#                                      str(k) +'" and obstype = "DARK" and filename like "%dark%" order by diff']
#                                 darkgood=pylcogt.utils.pymysql.query(command, conn)
#                                 if len(darkgood)>=1:
#                                     masterdarkname=darkgood[0]['filepath']+darkgood[0]['filename']
#                                     pylcogt.utils.lcogtsteps.run_applydark(listimg, outfilenames, masterdarkname, True)
#                                     for img in outfilenames:
#                                         print img
#                                         pylcogt.utils.pymysql.updateheader(img,0, {'DARKCOR':[string.split(masterdarkname,'/')[-1],' dark frame']})
#                                         siteid=pyfits.getheader(img)['SITEID']
#                                         directory=pylcogt.utils.pymysql.workingdirectory+siteid+'/'+k+'/'+re.sub('-','',str(j))+'/'
#                                         if not os.path.isdir(directory):
#                                             os.mkdir(directory)
#                                         print 'mv '+img+' '+directory
#                                         pylcogt.utils.lcogtsteps.ingest([img], 'lcogtredu', 'no')
#                                         os.system('mv '+img+' '+directory)
#             else:
#                 print 'no exposures selected'
#
#         elif _stage == 'applyflat':
#             print 'apply flat to science frame'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 listbin=ll0['ccdsum'][ww]
#                 listinst=ll0['instrument'][ww]
#                 listday=ll0['dayobs'][ww]
#                 listmjd=ll0['mjd'][ww]
#                 listfilt=ll0['filter'][ww]
#                 for filt in set(listfilt):
#                     for k in set(listinst):
#                         for i in set(listbin):
#                             for j in set(listday):
#                                 print j,k,i,filt
#                                 listimg=np.array(listfile)[(listbin == i) &  (listinst==k) & (listday == j) & (listfilt == filt)]
#                                 listmjd0=np.array(listmjd)[(listbin == i) &  (listinst==k) & (listday == j) & (listfilt == filt)]
#
#                                 #    select only images where flat was not apply
#                                 jj = np.asarray([ii for ii in range(0,len(listimg))
#                                              if not pyfits.getheader(listimg[ii]).get('FLATCOR')])
#                                 if len(jj):
#                                     listimg=np.array(listimg)[jj]
#                                     listmjd0=np.array(listmjd0)[jj]
#                                 else:
#                                     listimg=[]
#
#                                 if len(listimg)>0:
#                                     command=['select filepath,filename, mjd-'+str(listmjd0[0])+' as diff from lcogtredu where ccdsum="'+\
#                                          str(i)+'" and instrument = "'+\
#                                          str(k)+'" and filter = "'+str(filt)+'" and obstype="SKYFLAT" and filename like "%flat%" order by diff']
#                                     flatgood=pylcogt.utils.pymysql.query(command, conn)
#                                     if len(flatgood) >= 1:
#                                         masterflatname = flatgood[0]['filepath']+flatgood[0]['filename']
#                                         print masterflatname
#                                         print listimg
#                                         print 'apply flat to science frame'
#                                         pylcogt.utils.lcogtsteps.run_applyflat(listimg, listimg, masterflatname, True)
#                                         for img in listimg:
#                                             pylcogt.utils.pymysql.updateheader(img,0, {'FLATCOR':[string.split(masterflatname,'/')[-1],' flat frame']})
#                                     else:
#                                         print 'no flat for this setup '+str(filt)+' '+str(i)
#             else:
#                 print 'no exposures selected'
#
#         elif _stage == 'cosmic':
#             print 'select science images and correct for cosmic'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#
#                 #    select only images where flat, bias and dark are applyed
#                 jj = np.asarray([ii for ii in range(0,len(listfile))
#                              if (pyfits.getheader(listfile[ii]).get('BIASCOR')) and
#                                  pyfits.getheader(listfile[ii]).get('DARKCOR') and
#                                  pyfits.getheader(listfile[ii]).get('FLATCOR') ])
#                 listfile = np.array(listfile)[jj]
#                 outfilenames = [re.sub('.fits','.bpm.fits',ii)   for ii in listfile]
#                 pylcogt.utils.lcogtsteps.run_crreject(listfile, outfilenames, clobber=True)
#
#         elif _stage == 'wcs':
#             print 'select science image and do astrometry'
#             ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in ['EXPOSE'])])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#
#                 #    select only images where flat, bias and dark are applyed
#                 jj = np.asarray([ii for ii in range(0,len(listfile))
#                              if (pyfits.getheader(listfile[ii]).get('BIASCOR')) and
#                                  pyfits.getheader(listfile[ii]).get('DARKCOR') and
#                                  pyfits.getheader(listfile[ii]).get('FLATCOR') ])
#                 listfile = np.array(listfile)[jj]
#                 #outfilenames = [re.sub('.fits','.bpm.fits',ii)   for ii in listfile]
#                 pylcogt.utils.lcogtsteps.run_astrometry(listfile, listfile, clobber=True)
#                 for im in listfile:
#                     if pyfits.getheader(im).get('IMAGEH'):
#                         pylcogt.utils.pymysql.updateheader(im,0,
#                                                            {'WCSERR':[0,' ASTROMETRY'],
#                                                             'ASTROMET': ['1 1 1', 'rmsx rmsy nstars']
#                                                             })
#                     else:
#                         pylcogt.utils.pymysql.updateheader(im,0, {'WCSERR':[1,' ASTROMETRY']
#                                                                   })
#             else:
#                 print 'no exposures selected'
#         elif _stage == 'checkimg':
#             if _type:
#                 ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in [_type])])
#             else:
#                 ww = np.asarray([i for i in range(len(ll0['filename']))])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 from pyraf import iraf
#                 for img in listfile:
#                     iraf.display(img,frame=1, fill='yes')
#                     print img
#                     answ = raw_input('good [y/n] [y] ')
#                     if answ in ['n','No','NO']:
#                         answ='n'
#                         print 'delete image'
#                         os.system('mv '+img+' '+'/nethome/supernova/pylcogt/bad/')
#                         gg=pylcogt.utils.pymysql.getlistfromraw(conn, _table, 'filename', string.split(img,'/')[-1],
#                                                                 '', column2='*', telescope='all')
#                         if len(gg):
#                             command = ['delete from lcogtredu where filename = "'+string.split(img,'/')[-1]+'"']
#                             deletefromdb=pylcogt.utils.pymysql.query(command, conn)
#                             print deletefromdb
#         elif _stage == 'hdupdate':
#             if _type:
#                 ww = np.asarray([i for i in range(len(ll0['filename'])) if (ll0['obstype'][i] in [_type])])
#             else:
#                 ww = np.asarray([i for i in range(len(ll0['filename']))])
#             if ww.size:
#                 listfile = [k + v for k, v in zip(ll0['filepath'][ww], ll0['filename'][ww])]
#                 pylcogt.utils.lcogtsteps.run_hdupdate(listfile)
    logs.stop_logging()