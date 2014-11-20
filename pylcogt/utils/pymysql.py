##############################################################################################
#
#          here we define the filepath where raw data and reduced data will end
#

workingdirectory = '/science/supernova/pylcogt'
rawdata = '/archive/engineering/'
realpass = 'configure'

#
#################################################################################################

def readpasswd(directory,_file):
    from numpy import genfromtxt
    data=genfromtxt(directory+_file,str)
    gg={}
    for i in data:
        try:
            gg[i[0]]=eval(i[1])
        except:
            gg[i[0]]=i[1]
    return gg

################################################################################################

readpass=readpasswd(workingdirectory,realpass)

def getconnection(site):
   '''
   this is reading the database information from the configuration file
   '''
   import  pylcogt

   connection={}
   connection[site]['database']=pylcogt.pymysql.readpass['database']
   connection[site]['hostname']=pylcogt.pymysql.readpass['hostname']
   connection[site]['username']=pylcogt.pymysql.readpass['mysqluser']
   connection[site]['passwd']=pylcogt.pymysql.readpass['mysqlpasswd']
   return  connection[site]['hostname'],connection[site]['username'],connection[site]['passwd'],connection[site]['database']

#################################################################################################################

def dbConnect(lhost, luser, lpasswd, ldb):
   import sys
   import MySQLdb
   try:
      conn = MySQLdb.connect (host = lhost,
                              user = luser,
                            passwd = lpasswd,
                                db = ldb)
   except MySQLdb.Error, e:
      print "Error %d: %s" % (e.args[0], e.args[1])
      sys.exit (1)
   return conn

###########################################################################

def insert_values(conn,table,values):
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
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)
        cursor.execute(sql, values)
        resultSet = cursor.fetchall ()
        if cursor.rowcount == 0:
            pass
        cursor.close ()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])

############################################################################


def insert_values(conn,table,values):
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
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)
        cursor.execute(sql, values)
        resultSet = cursor.fetchall ()
        if cursor.rowcount == 0:
            pass
        cursor.close ()
    except MySQLdb.Error, e:
        print "Error %d: %s" % (e.args[0], e.args[1])

###########################################################################

def updateheader(image,dimension,headerdict):
    import pyfits
    try:
        imm=pyfits.open(image,mode='update')
        _header=imm[dimension].header
        for i in headerdict.keys():
           _header.update(i,headerdict[i][0],headerdict[i][1])
        imm.flush()
        imm.close()
    except:
        print 'warning: problem to update header, try to correct header format ....'

#############################################

def query(command,conn):
   import MySQLdb
   list=''
   try:
        cursor = conn.cursor (MySQLdb.cursors.DictCursor)
        for i in command:
            cursor.execute (i)
            list = cursor.fetchall ()
            if cursor.rowcount == 0:
                pass
        cursor.close ()
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
    if site in ['lsc','elp','coj','cpt','ogg']:
        site = [site]
    else:
        site = ['lsc','elp','coj','cpt','ogg']

    ascifile='http://pydevsba.lco.gtn/configdb/camera_mappings/'
    line = 'wget '+ascifile+' -O configtel.txt'
    os.system(line)
    data = np.genfromtxt('configtel.txt',str)
    vector = ['Site','Observatory','Telescope','Camera',\
                'CameraType', 'Size', 'PScale', 'BinningAvailable',
            'Std.Binning', 'Overhead', 'Autoguider', 'AutoguiderType', 'Filters']
    aa=zip(*data)
    dict = {}
    for i in range(0,len(vector)):
        dict[vector[i]] = np.array(aa[i])
    goodcamera = [i for i in range(len(dict['CameraType'])) if dict['CameraType'][i] in [\
            '1m0-SciCam-Sinistro','1m0-SciCam-SBIG','2m0-SciCam-Spectral']]

    dict1 = {}
    for i in dict:
        dict1[i] = dict[i][goodcamera]
    instrument = []
    for i in site:
        instrument = list(instrument) + list(dict1['Camera'][dict1['Site'] == i])
    telescope=[]
    for i in site:
        telescope = list(telescope) + list(dict1['Observatory'][dict1['Site'] == i])
    _site = []
    for i in site:
        _site = list(_site)+list(dict1['Site'][dict1['Site'] == i])
    return dict1, instrument, telescope, _site

#######################################################################
