# #############################################################################################
#
# here we define the filepath where raw data and reduced data will end
#
import numpy as np
import string
import re

workingdirectory = '/nethome/supernova/pylcogt/'
rawdata = '/archive/engineering/'
realpass = 'configure'

instrument0 = {'sbig': ['kb05', 'kb70', 'kb71', 'kb73', 'kb74', 'kb75', 'kb76', 'kb77', 'kb78', 'kb79'],
               'sinistro': ['fl02', 'fl03', 'fl04', 'fl05', 'fl06', 'fl07', 'fl08', 'fl09', 'fl10'],
               'spectral': ['fs02', 'fs03', 'fs01', 'em01', 'em02']}
instrument0['all'] = list(instrument0['sbig']) + list(instrument0['sinistro']) + list(instrument0['spectral'])

telescope0 = {'lsc': ['1m0-04', '1m0-05', '1m0-09'], 'elp': ['1m0-08'], 'cpt': ['1m0-10', '1m0-12', '1m0-13'],
              'coj': ['1m0-11', '1m0-03', '2m0-02'], 'ogg': ['2m0-01'], 'all': ['1m0-03', '1m0-04', '1m0-05',
                                                                                '1m0-08', '1m0-09', '1m0-10',
                                                                                '1m0-11', '1m0-12', '1m0-13',
                                                                                '2m0-01', '2m0-02']}
site0 = ['lsc', 'elp', 'coj', 'cpt', 'ogg']

dome0 = {('lsc', 'domc'): '1m0-04', ('lsc', 'doma'): '1m0-05', ('lsc', 'domb'): '1m0-09', ('elp', 'doma'): '1m0-08',
         ('cpt', 'doma'): '1m0-10', ('cpt', 'domc'): '1m0-12', ('cpt', 'domb'): '1m0-13', ('coj', 'doma'): '1m0-11',
         ('coj', 'domb'): '1m0-03', ('coj', 'clma'): '2m0-02', ('ogg', 'clma'): '2m0-01'}


#create table lcogtraw (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, filename VARCHAR(50) UNIQUE KEY,
# filepath VARCHAR(100), object VARCHAR(50), mjd FLOAT, dateobs DATE, dayobs DATE, exptime FLOAT, filter VARCHAR(20),
# grism VARCHAR(20), telescope VARCHAR(20), instrument VARCHAR(20), obstype VARCHAR(20),
# airmass FLOAT, ut TIME, slit VARCHAR(20), lamp VARCHAR(20), ra0 FLOAT, dec0 FLOAT,
# userid  VARCHAR(20), propid VARCHAR(20), groupid VARCHAR(20), tracknum   BIGINT,  reqnum BIGINT;

#################################################################################################

def readpasswd(directory, _file):
    from numpy import genfromtxt

    data = genfromtxt(directory + _file, str)
    gg = {}
    for i in data:
        try:
            gg[i[0]] = eval(i[1])
        except:
            gg[i[0]] = i[1]
    return gg

################################################################################################

readpass = readpasswd(workingdirectory, realpass)


def getconnection():
    '''
    this is getting the connection to the database using the configuration file
    '''
    import pylcogt

    dd = pylcogt.utils.pymysql.readpass['database']
    hh = pylcogt.utils.pymysql.readpass['hostname']
    uu = pylcogt.utils.pymysql.readpass['mysqluser']
    pw = pylcogt.utils.pymysql.readpass['mysqlpasswd']
    conn = dbConnect(hh, uu, pw, dd)
    return conn


#################################################################################################################

def dbConnect(lhost, luser, lpasswd, ldb):
    import sys
    import MySQLdb

    try:
        conn = MySQLdb.connect(host=lhost,
                               user=luser,
                               passwd=lpasswd,
                               db=ldb)
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
    return conn


###########################################################################

def insert_values(conn, table, values):
    import MySQLdb

    def dictValuePad(key):
        return '%(' + str(key) + ')s'

    def insertFromDict(table, dict):
        """Take dictionary object dict and produce sql for
        inserting it into the named table"""
        sql = 'INSERT INTO ' + table
        sql += ' ('
        sql += ', '.join(dict)
        sql += ') VALUES ('
        sql += ', '.join(map(dictValuePad, dict))
        sql += ');'
        return sql

    sql = insertFromDict(table, values)
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(sql, values)
        resultSet = cursor.fetchall()
        if cursor.rowcount == 0:
            pass
        cursor.close()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])


############################################################################


def updatevalue(conn, table, column, value, namefile, namefile0='filename'):
    import MySQLdb

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        if value in [True, False]:
            cursor.execute("UPDATE " + str(table) + " set " + column + "=" + str(value) + " where " + str(
                namefile0) + "= " + "'" + str(namefile) + "'" + "   ")
        else:
            cursor.execute("UPDATE " + str(table) + " set " + column + "=" + "'" + str(value) + "'" + " where " + str(
                namefile0) + "= " + "'" + str(namefile) + "'" + "   ")
        resultSet = cursor.fetchall()
        if cursor.rowcount == 0:
            pass
        cursor.close()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])


###########################################################################

def updateheader(image, dimension, headerdict):
    import pyfits

    try:
        imm = pyfits.open(image, mode='update')
        _header = imm[dimension].header
        for i in headerdict.keys():
            _header[i]=(headerdict[i][0], headerdict[i][1])
            #_header.update(i, headerdict[i][0], headerdict[i][1])
        imm.flush()
        imm.close()
    except:
        print 'warning: problem to update header, try to correct header format ....'


#############################################

def query(command, conn):
    import MySQLdb

    list = ''
    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        for i in command:
            cursor.execute(i)
            list = cursor.fetchall()
            if cursor.rowcount == 0:
                pass
        cursor.close()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
    return list


############################################################

def cameraavailable(site=''):
    '''
    This definition is interrogating the camera mapping to know which telescopes are available
    if site is specified
    '''
    import numpy as np
    import os

    if site in ['lsc', 'elp', 'coj', 'cpt', 'ogg']:
        site = [site]
    else:
        site = ['lsc', 'elp', 'coj', 'cpt', 'ogg']

    ascifile = 'http://pydevsba.lco.gtn/configdb/camera_mappings/'
    line = 'wget ' + ascifile + ' -O configtel.txt'
    os.system(line)
    data = np.genfromtxt('configtel.txt', str)
    vector = ['Site', 'Observatory', 'Telescope', 'Camera', \
              'CameraType', 'Size', 'PScale', 'BinningAvailable',
              'Std.Binning', 'Overhead', 'Autoguider', 'AutoguiderType', 'Filters']
    aa = zip(*data)
    dict = {}
    for i in range(0, len(vector)):
        dict[vector[i]] = np.array(aa[i])
    goodcamera = [i for i in range(len(dict['CameraType'])) if dict['CameraType'][i] in [ \
        '1m0-SciCam-Sinistro', '1m0-SciCam-SBIG', '2m0-SciCam-Spectral']]

    dict1 = {}
    for i in dict:
        dict1[i] = dict[i][goodcamera]
    instrument = []
    for i in site:
        instrument = list(instrument) + list(dict1['Camera'][dict1['Site'] == i])
    telescope = []
    for i in site:
        telescope = list(telescope) + list(dict1['Observatory'][dict1['Site'] == i])
    _site = []
    for i in site:
        _site = list(_site) + list(dict1['Site'][dict1['Site'] == i])
    return dict1, instrument, telescope, _site


#######################################################################

def readkey(hdr, keyword):
    import pyfits
    import re
    import string

    try:
        _instrume = hdr.get('INSTRUME').lower()
    except:
        _instrume = 'none'

    if _instrume in ['kb05', 'kb70', 'kb71', 'kb73', 'kb74', 'kb75', 'kb76', 'kb77', 'kb78', 'kb79']:  # SBIG
        useful_keys = {'object': 'OBJECT',
                       'date-obs': 'DATE-OBS',
                       'ut': 'DATE-OBS',
                       'RA': 'RA',
                       'DEC': 'DEC',
                       'datamin': -100.0,
                       'datamax': 'SATURATE',
                       'observer': 'OBSERVER',
                       'exptime': 'EXPTIME',
                       'wcserr': 'WCSERR',
                       'instrume': 'INSTRUME',
                       'MJD': 'MJD-OBS',
                       'filter': 'FILTER',
                       'gain': 'GAIN',
                       'ron': 'RDNOISE',
                       'airmass': 'AIRMASS',
                       'type': 'OBSTYPE',
                       'propid': 'PROPID',
                       'telescop': 'TELESCOP'}
    elif _instrume in ['fl02', 'fl03', 'fl04', 'fl05', 'fl06']:  # sinistro
        useful_keys = {'object': 'OBJECT',
                       'date-obs': 'DATE-OBS',
                       'ut': 'DATE-OBS',
                       'RA': 'RA',
                       'DEC': 'DEC',
                       'datamin': -100.0,
                       'datamax': 'SATURATE',
                       'observer': 'OBSERVER',
                       'exptime': 'EXPTIME',
                       'wcserr': 'WCSERR',
                       'instrume': 'INSTRUME',
                       'MJD': 'MJD-OBS',
                       'filter': 'FILTER',
                       'gain': 'GAIN',
                       'ron': 'RDNOISE',
                       'airmass': 'AIRMASS',
                       'type': 'OBSTYPE',
                       'propid': 'PROPID',
                       'telescop': 'TELESCOP'}
    elif _instrume in ['fs01', 'em03', 'fs02', 'em01', 'fs03']:
        useful_keys = {'object': 'OBJECT',
                       'date-obs': 'DATE-OBS',
                       'ut': 'DATE-OBS',
                       'RA': 'RA',
                       'DEC': 'DEC',
                       'datamin': -100,
                       'datamax': 60000,
                       'wcserr': 'WCS_ERR',
                       'observer': 'OBSERVER',
                       'exptime': 'EXPTIME',
                       'instrume': 'INSTRUME',
                       'MJD': 'MJD-OBS',
                       'filter': 'FILTER',
                       'gain': 'GAIN',
                       'ron': 'RDNOISE',
                       'airmass': 'AIRMASS',
                       'type': 'OBSTYPE',
                       'telescop': 'TELESCOP'}
    else:
        useful_keys = {'object': 'OBJECT',
                       'date-obs': 'DATE-OBS'}

    if keyword in useful_keys:
        if type(useful_keys[keyword]) == float:
            value = useful_keys[keyword]
        else:
            value = hdr.get(useful_keys[keyword])
            if keyword == 'date-obs':
                try:
                    value = re.sub('-', '', string.split(value, 'T')[0])
                except:
                    pass
            elif keyword == 'ut':
                try:
                    value = string.split(value, 'T')[1]
                except:
                    pass
            elif keyword == 'object':
                #             value=re.sub('\}','',value)
                #             value=re.sub('\{','',value)
                #             value=re.sub('\[','',value)
                #             value=re.sub('\]','',value)
                #             value=re.sub('\(','',value)
                #             value=re.sub('\)','',value)
                #             value=re.sub('-','',value)
                value = re.sub(' ', '', value)
            elif keyword == 'instrume':
                value = value.lower()
            elif keyword == 'filter':

                import string, re

                value1 = hdr.get('FILTER2')
                value2 = hdr.get('FILTER1')
                value3 = hdr.get('FILTER3')
                value = [a for a in [value1, value2, value3] if 'air' not in a]

                print value
                if not value:
                    value = 'air'
                else:
                    value = value[0]
            elif keyword == 'RA':
                import string, re
                print value
                if value!='NaN':
                    value = (((float(string.split(value, ':')[2]) / 60 + float(string.split(value, ':')[1])) / 60) \
                         + float(string.split(value, ':')[0])) * 15
            elif keyword == 'DEC':
                import string, re
                if value!='NaN':
                    if string.count(string.split(value, ':')[0], '-') == 0:
                        value = ((float(string.split(value, ':')[2]) / 60 + float(string.split(value, ':')[1])) / 60) \
                                + float(string.split(value, ':')[0])
                    else:
                        value = (-1) * (
                            ((abs(float(string.split(value, ':')[2]) / 60) + float(string.split(value, ':')[1])) / 60) \
                            + abs(float(string.split(value, ':')[0])))
    else:
        if keyword in hdr:
            value = hdr.get(keyword)
        else:
            value = ''
    if type(value) == str:
        value = re.sub('\#', '', value)
    return value


####################################################################################################################

def getlistfromraw(conn, table, column, value1, value2, column2='*', telescope='all'):

    import sys
    import pylcogt
    import MySQLdb
    import string

    try:
        cursor = conn.cursor(MySQLdb.cursors.DictCursor)
        if telescope == 'all':
            if value2:
                cursor.execute("select " + column2 + " from " + str(
                    table) + " where " + column + "<=" + "'" + value2 + "' and " + column + ">=" + "'" + value1 + "'")
            else:
                cursor.execute(
                    "select " + column2 + " from " + str(table) + " where " + column + "=" + "'" + value1 + "'")
        elif telescope in pylcogt.utils.pymysql.site0 + ['1m0', 'fl', 'kb', '2m0']:
            if value2:
                cursor.execute("select " + column2 + " from " + str(
                    table) + " where " + column + "<=" + "'" + value2 + "' and " + column + ">=" + "'" + value1 +
                               "' and filename like '%" + telescope + "%'")
            else:
                cursor.execute("select " + column2 + " from " + str(
                    table) + " where " + column + "=" + "'" + value1 + "' and filename like '%" + telescope + "%'")
        else:
            if value2:
                cursor.execute("select " + column2 + " from " + str(table) + " where " + column + "<=" + "'" +
                               value2 + "' and " + column + ">=" + "'" + value1 + "' and telescope='" + telescope + "'")
            else:
                cursor.execute("select " + column2 + " from " + str(
                    table) + " where " + column + "=" + "'" + value1 + "' and  telescope='" + telescope + "'")
        resultSet = cursor.fetchall()
        if cursor.rowcount == 0:
            pass
        cursor.close()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
    return resultSet

    ################################################################################


def filtralist(ll2, _filter, _id, _name, _ra, _dec, _bin):

    ll1 = {}
    for key in ll2.keys():
        ll1[key] = ll2[key][:]

    if _filter:  #       select by filter
        if _filter == 'sloan':
            ww = np.asarray([i for i in range(len(ll1['filter'])) if (
            (ll1['filter'][i] in ['zs', 'up', 'gp', 'ip', 'rp', 'SDSS-G', 'SDSS-R', 'SDSS-I', 'Pan-Starrs-Z']))])
        elif _filter == 'landolt':
            ww = np.asarray([i for i in range(len(ll1['filter'])) if (
            (ll1['filter'][i] in ['U', 'B', 'V', 'R', 'I', 'Bessell-B', 'Bessell-V', 'Bessell-R', 'Bessell-I']))])
        elif _filter == 'apass':
            ww = np.asarray([i for i in range(len(ll1['filter'])) if ((ll1['filter'][i] in ['B', 'V', 'Bessell-B',
                                                                                         'Bessell-V', 'gp', 'ip', 'rp',
                                                                                         'SDSS-G', 'SDSS-R',
                                                                                         'SDSS-I']))])
        elif _filter in ['w','zs', 'up', 'gp', 'ip', 'rp', 'U', 'B', 'V', 'R', 'I', 'SDSS-G', 'SDSS-R', 'SDSS-I',
                         'Pan-Starrs-Z', 'Bessell-B', 'Bessell-V', 'Bessell-R', 'Bessell-I']:
            ww = np.asarray([i for i in range(len(ll1['filter'])) if ((ll1['filter'][i] in [_filter]))])
        else:
            ww=np.asarray([])
#            lista = []
#            for fil in _filter:
#                try:
#                    lista.append(agnkey.sites.filterst('lsc')[fil])
#                except:
#                    try:
#                        lista.append(agnkey.sites.filterst('fts')[fil])
#                    except:
#                        pass
#            ww = asarray([i for i in range(len(ll1['filter'])) if ((ll1['filter'][i] in lista))])
        if len(ww) > 0:
            for jj in ll1.keys():
                ll1[jj] = np.array(ll1[jj])[ww]
        else:
            for jj in ll1.keys():
                ll1[jj] = []

    if _bin:  #    select only one type of binning
        ww = np.asarray([i for i in range(len(ll1['filter'])) if ( (re.sub("x"," ",_bin) in ll1['ccdsum'][i]))])
        if len(ww) > 0:
            for jj in ll1.keys():
                ll1[jj] = np.array(ll1[jj])[ww]
        else:
            for jj in ll1.keys():
                ll1[jj] = []

    if _id:  # select by ID
#        try:
#            xx = '0000'[len(_id):] + _id
#            ww = np.asarray([i for i in range(len(ll1['filter'])) if ((_id in string.split(ll1['filename'][i], '-')[3]))])
#        except:
        ww = np.asarray([i for i in range(len(ll1['filter'])) if (_id in ll1['filename'][i])])
        if len(ww) > 0:
            for jj in ll1.keys():
                ll1[jj] = np.array(ll1[jj])[ww]
        else:
            for jj in ll1.keys():
                ll1[jj] = []
    if _name:  #    select by name
        ww = np.asarray([i for i in range(len(ll1['filter'])) if ((_name in ll1['object'][i]))])
        if len(ww) > 0:
            for jj in ll1.keys():
                ll1[jj] = np.array(ll1[jj])[ww]
        else:
            for jj in ll1.keys():
                ll1[jj] = []
    if _ra and _dec:        #    select using ra and dec
        ww = np.asarray([i for i in range(len(ll1['ra0'])) if (
        np.abs(float(ll1['ra0'][i]) - float(_ra)) < .5 and abs(float(ll1['dec0'][i]) - float(_dec)) < .5 )])
        if len(ww) > 0:
            for jj in ll1.keys():
                ll1[jj] = np.array(ll1[jj])[ww]
        else:
            for jj in ll1.keys():
                ll1[jj] = []
    return ll1

#################################################################################################
