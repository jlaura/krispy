import ast
import cPickle
import decimal
import glob
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import time

import numpy as np

from flask import Blueprint, request, jsonify, g, current_app
from flask.ext.login import login_required, current_user
from werkzeug.utils import secure_filename

import geoalchemy2.functions as geofuncs

from app import db, seen_classes, cachedobjs
from app.mod_data.models import UserData, UserPyObj, GeoPoly, GEOMLOOKUP
from app.mod_data import upload_helpers as uph
import config

mod_data = Blueprint('mod_data', __name__)

def getdatalist(cuid, tabular = True):
    cuid = int(cuid)
    if tabular:
    	availabledata = UserData.query.filter_by(userid = cuid).all()
    else:
    	availabledata = UserPyObj.query.filter_by(userid = cuid).all()
    entries = {}
    for a in availabledata:
        dataname = a.datahash.split('_')
        entry = {'name':a.dataname,
                'href':config.baseurl + '/data/{}/'.format(a.datahash)}
	#Expiration time also goes here.
        entries[a.id] = entry
    return entries

@mod_data.route('/', methods=['GET'])
@login_required
def listdata():
    """
    List the available datasets by querying the DB and
    returning metadata about the available user uploaded data.
    """
    cuid = current_user.get_id()
    response = {'status':'success','links':[]}
    links = response['links']
    for i in getdatalist(cuid).itervalues():
	response['links'].append(i)
    for i in getdatalist(cuid, tabular=False).itervalues():
	response['links'].append(i)
    response['links'].append({'name':'upload', 'href':config.baseurl + '/data/upload/'})	 
    return jsonify(response)

@mod_data.route('/nontabular/', methods=['GET'])
@login_required
def list_nontabular_data():
    cuid = current_user.get_id()
    response = {'status':'success', 'data':{'nontabular'}}
    response['data'] = getdatalist(cuid, tabular=False)
    return jsonify(response)

@mod_data.route('/tabular/', methods=['GET'])
@login_required
def list_tabular_data():
    cuid = current_user.get_id()
    response = {'status':'success', 'data':{'nontabular'}}
    response['data'] = getdatalist(cuid)
    return jsonify(response)


def parse_nontabular(response, row):
    """
    Parse a row containing a nontabular data entry, e.g. a PySAL object,
    and return a listing of available methods and attributes
    """
    #response['data']['name'] = row.dataname
    #response['data']['date_created'] = row.date_created
    #response['data']['date_last_accessed'] = row.date_accessed
    #Expiration goes here as maxstorage time - row.data_accessed

    row.get_pyobj()
    response['links']['fields']['methods'] = row.methods
    response['links']['fields']['attributes'] = row.attributes
    response['links']['fields']['attributes'].append('full_result')
    response['links']['fields']['provenance'] = {}

    return response


def parse_tabular(response, tablename, tablehash):
    """
    Open a table containing tabular data and return a listing of fields
    """
    if tablehash in seen_classes:
        cls = current_app.class_references[tablehash]
    else:
        db.metadata.reflect(bind=db.engine)
        seen_classes.add(tablehash)
        #Dynamic class creation using metaclasses	 
        geomtype = "Polygon"
	basegeomcls = GEOMLOOKUP[geomtype]
 	cls = type(str(tablehash), (basegeomcls, db.Model,), {'__tablename__':tablehash,
                '__table_args__' : {'extend_existing': True}})
        current_app.class_references[tablehash] = cls

    #response['data']['name'] = tablename
    response['links'][2] = {'fields': [c.name for c in cls.__table__.columns]}
    #response['data']['fields'].append('geojson')
    #TODO: Add topojson support if the db is postgresql
    
    return response

@mod_data.route('/<objhash>/', methods=['GET'])
#@login_required
def get_cached_entry(objhash):
    response = {'status':'success',
		'methods': ['GET', 'POST','PUT', 'DELETE'],
		'links':[{'name':'raw', 'href':'{}/data/{}/raw/'.format(config.baseurl, objhash)},
			 {'name':'geojson', 'href':'{}/data/{}/geojson/'.format(config.baseurl, objhash)},
			 {'name':'fields','href':'{}/data/{}/fields/'.format(config.baseurl, objhash)}]}
   
    '''	 
    row = UserPyObj.query.filter_by(datahash = objhash).first()
    if row != None:
	response = parse_nontabular(response, row)
    else:
	row = UserData.query.filter_by(datahash = objhash).first()
	tablehash = row.datahash
	tablename = row.dataname
	response = parse_tabular(response, tablename, tablehash)
    '''

    return jsonify(response)


@mod_data.route('/<objhash>/<value>/', methods=['GET'])
#@login_required
def get_stored_entry(objhash, value):
    """
    This is a dispatcher function which dispatches the request to either a function
    to return an value of an object or a field of a table.
    """
    response = {'status':'success','data':{}}

    row = UserPyObj.query.filter_by(datahash = objhash).first()
    if row != None:
    	row.get_pyobj()
	if value != 'full_result' and value != 'raw':	
	    try:
		responsedata =  getattr(row.liveobj, value)
	        if isinstance(responsedata, np.ndarray):
			responsedata = responsedata.tolist()
		response['data'] = responsedata	
		return jsonify(response)
	    except:
		return jsonify({'status':'failure', 'data':'Unable to find value'})
	elif value == 'raw':
	    try:
	        data = row.pyobj
	        if isinstance(data, np.ndarray):
		    return json.dumps(data.tolist())
	        else:
		    return cPickle.dumps(row.pyobj)
	    except:
		response = {'status':'error', 'message':'The object has no raw representation.'}
		return jsonify(response)
	else:
	    #serialize the full row
	    pass
    else:
	response = get_dataset_field(objhash, value)
	return jsonify(response)



@mod_data.route('/cached/<uid>/<objhash>/<method>', methods=['POST'])
@login_required
def call_cached_centry_method(uid, objhash, method):
    raise NotImplementedError


def get_dataset_field(tablename, field):
    response = {'status':'success','data':{}}
    if tablename in seen_classes:
        cls = current_app.class_references[tablename]
    else:
        db.metadata.reflect(bind=db.engine)
        seen_classes.add(tablename)
        cls = type(str(tablename), (GeoPoly, db.Model,), {'__tablename__':tablename,
           '__table_args__' : {'extend_existing': True}})
        current_app.class_references[tablename] = cls

    if field == config.geom_column:
        vector = cls.query.with_entities(geofuncs.ST_AsGeoJSON(getattr(cls, field))).all()
        response['data'] = [v[0] for v in vector]
    elif field == 'geojson':
        #TODO: How can this be cleaner?  Do I need 2 queries go get geojson?
        #rows = cls.query.all()
        geoms = cls.query.with_entities(geofuncs.ST_AsGeoJSON(getattr(cls, config.geom_column))).all()
	features = []
        for i, row in enumerate(geoms):
            #attributes = row.as_dict()
            #attributes.pop('wkb_geometry', None)
            #for k, v in attributes.iteritems():
     	        #if isinstance(v, decimal.Decimal):
   	            #attributes[k] = float(v)
            current_feature = {'type':'Feature',
                    'geometry':ast.literal_eval(geoms[i][0])}
                    #'properties':attributes}
            features.append(current_feature)
        geojson = {"type": "FeatureCollection","features": features}
	#geojson = {"type":"FeatureCollection", "features": geoms}
        response['data']['geojson'] = geojson
    elif field == 'topojson':
        #TODO: Add topojson support if the DB is postgresql
        pass
    elif field == 'raw':
	return {'status':'error', 'message':'Tabular data does not have a raw representation, yet.'}
    else:
        vector = cls.query.with_entities(getattr(cls, field)).all()
        responsevector = [v[0] for v in vector]
        if isinstance(responsevector[0], decimal.Decimal):
    	    for i, v in enumerate(responsevector):
     	        responsevector[i] = float(v)
        response['data'] = responsevector
    return response


@mod_data.route('/upload/', methods=['GET'])
@login_required
def upload_get():
    """
    The upload homepage.
    """
    response = {'status':'success'}
    response['methods'] = ['GET', 'POST']
    response['arguments'] = {}
    response['arguments']['required_arguments'] = {'filename':{'description':'.zip file containing ogr2ogr convertables spatial data',
                                                    'default': 'None',
                                                    'type': 'file object'}}

    response['description'] = "Endpoint to upload data to this data service using a POST request."
    return jsonify(response)

@mod_data.route('/upload/', methods=['POST'])
@login_required
def upload():
    """
    Upload to a temporary directory, validate, call ogr2ogr and write to the DB

    Using curl via the command line.
    ---------------------------------
    Example 1 is from pysal examples (from that dir)
    Example 2 is a subset of NAT, zipped.
    Example 3 is columbus via the webpool and a sample user.
    curl -X POST -F shp=@columbus.shp -F shx=@columbus.shx -F dbf=@columbus.dbf http://localhost:8080/mydata/upload/
    curl -X POST -F filename=@NAT_Subset.zip http://localhost:8080/mydata/upload/
    curl -i -k -u jay@jay.com:jay -X POST -F filename=@col.zip  https://webpool.csf.asu.edu/pysalrest/mydata/upload/
    """

    cuid = current_user.get_id()
    tmpdir = tempfile.mkdtemp()
    for f in request.files.values():
        if f and uph.allowed_file(f.filename):
            filename = secure_filename(f.filename)
            savepath = os.path.join(tmpdir, filename)
            f.save(savepath)

            basename, ext = filename.split('.')
            if ext == 'zip':
                uph.unzip(savepath, tmpdir)

    #Now iterate over all the shapefiles and call ogr2ogr
    shps = glob.glob(os.path.join(tmpdir, '*.shp'))
    for shp in shps:
        shptablename = os.path.splitext(os.path.basename(shp))[0]
	#datahashvalue = '{}_{}_{}'.format(cuid, shptablename, time.time())
	datahash = hashlib.sha1(shp).hexdigest()
	host, port = config.dbhost.split(':')
        cmd = [config.ogr2ogr, '-f', "{}".format(config.dbtypename),
               "{}:host={} port={} user={} password={} dbname={}".format(config.dbabbrev,
                                                                 host,
                                                                 port,
                                                                 config.dbusername,
                                                                 config.dbpass,
                                                                 config.dbname),
               shp,
               '-nlt', 'PROMOTE_TO_MULTI',
               '-nln', datahash,
               '-lco', 'GEOMETRY_NAME={}'.format(config.geom_column),
		'-skipfailures']
        response = subprocess.call(cmd)

        uploadeddata = UserData(cuid, datahash, shptablename)
        db.session.add(uploadeddata)
        db.session.commit()

    #Cleanup
    shutil.rmtree(tmpdir)

    return jsonify({'status':'success', 'data':{'href': config.baseurl + '/data/{}'.format(datahash)}})
