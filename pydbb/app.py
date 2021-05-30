
import datetime
import json
import os

from bson.json_util import dumps, loads
from bson.objectid import ObjectId

from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

from pydbb.fields import convert_record, get_fields
from pydbb.mongo_db import admin_db, get_db
from pydbb.tables import create_indices, validate_table_name

app = Flask("pyDDB")

@app.route('/')
def home():
    return jsonify({ "ok": True })

@app.route('/<db_name>/_/tables/<table_name>', methods=['POST'])
def create_table(db_name, table_name):

    name_check = validate_table_name(table_name)

    if name_check[0] == False:
        return jsonify({
            'success': False,
            'message': name_check[1]
        })
    
    content = request.get_json()

    fields = get_fields(content)

    admin_db['tables'].insert_one({
        'db': db_name,
        'name': table_name,
        'full_name': f'{db_name}:{table_name}',
        'fields': fields
    })

    create_indices(get_db(db_name)[table_name], fields)

    return jsonify({ 'success': True })

@app.route('/<db_name>/_/ensure-table/<table_name>', methods=['POST'])
def ensure_table(db_name, table_name):

    name_check = validate_table_name(table_name)

    if name_check[0] == False:
        return jsonify({
            'success': False,
            'message': name_check[1]
        })
    
    content = request.get_json()

    fields = get_fields(content)

    full_table_name = f'{db_name}:{table_name}'

    data = {
        'db': db_name,
        'name': table_name,
        'full_name': full_table_name,
        'fields': fields
    }

    res = admin_db['tables'].update({'full_name': full_table_name}, data,
        upsert=True)

    create_indices(get_db(db_name)[table_name], fields)

    return jsonify({ 'success': True })

@app.route('/<db_name>/_/tables')
def list_tables(db_name):

    query = admin_db['tables'].find({
        'db': db_name
    })
    return dumps({
        'success': True,
        'results': [q for q in query]
    })

@app.route('/<db_name>/_/tables/<table_name>')
def get_table(db_name, table_name):

    query = admin_db['tables'].find_one({
        'db': db_name,
        'name': table_name
    })

    if not query:
        return jsonify({
            'success': False,
            'message': "Table not found."
        })

    return dumps({
        'success': True,
        'result': query
    })

@app.route('/<db_name>/_/tables/<table_name>', methods=['PUT'])
def edit_table(db_name, table_name):
    
    content = request.get_json()

    fields = get_fields(content)

    full_table_name = f'{db_name}:{table_name}'

    data = {
        'db': db_name,
        'name': table_name,
        'full_name': full_table_name,
        'fields': fields
    }

    admin_db['tables'].update({'full_name': full_table_name}, data)

    create_indices(get_db(db_name)[table_name], fields)

    return jsonify({ 'success': True })

@app.route('/<db_name>/_/tables/<table_name>', methods=['DELETE'])
def destroy_table(db_name, table_name):
    admin_db['tables'].remove({
        'db': db_name,
        'name': table_name
    })
    get_db(db_name)[table_name].drop()
    return jsonify({ 'success': True })

# query POST routes here

@app.route('/<db_name>/$/query/<table_name>/page/<page_num>', methods=['POST'])
def query_objects(db_name, table_name, page_num):

    query_obj = request.get_json()

    page = int(page_num)
    page_size = int(os.getenv('PAGE_SIZE', 100))

    requested_page_size = request.args.get('page_size')
    if requested_page_size:
        page_size = int(requested_page_size)

    d = get_db(db_name)[table_name]
    
    q = d.find(query_obj).skip((page - 1) * page_size).limit(page_size)

    schema = admin_db['tables'].find_one({
        'db': db_name,
        'name': table_name
    })['fields']

    return dumps({
        'success': True,
        'results': [convert_record(schema, e) for e in q]
    })

@app.route('/<db_name>/<table_name>/page/<page_num>')
def list_objects(db_name, table_name, page_num):

    page = int(page_num)
    page_size = int(os.getenv('PAGE_SIZE', 100))

    requested_page_size = request.args.get('page_size')
    if requested_page_size:
        page_size = int(requested_page_size)

    d = get_db(db_name)[table_name]
    
    q = d.find().skip((page - 1) * page_size).limit(page_size)

    schema = admin_db['tables'].find_one({
        'db': db_name,
        'name': table_name
    })['fields']

    return dumps({
        'success': True,
        'results': [convert_record(schema, e) for e in q]
    })

@app.route('/<db_name>/<table_name>/<object_id>')
def get_object(db_name, table_name, object_id):
    
    t = get_db(db_name)[table_name]
    q = t.find_one({ '_id': ObjectId(object_id) })

    if not q:
        return jsonify({
            'success': False,
            'message': "Not found."
        })

    schema = admin_db['tables'].find_one({
        'db': db_name,
        'name': table_name
    })['fields']

    return dumps({
        'success': True,
        'result': convert_record(schema, q)
    })

@app.route('/<db_name>/<table_name>/<object_id>', methods=['PATCH'])
def patch_object(db_name, table_name, object_id):

    t = get_db(db_name)[table_name]
    q = t.find_one({ '_id': ObjectId(object_id) })

    if not q:
        return jsonify({
            'success': False,
            'message': "Not found."
        })

    schema = admin_db['tables'].find_one({
        'db': db_name,
        'name': table_name
    })['fields']

    now = datetime.datetime.now()

    obj = request.get_json()

    q = dict(q)
    q = { **q, **obj, 'modified_date': now }

    res =  t.find_one_and_update(
        { '_id': ObjectId(object_id) },
        { '$set': q }
    )

    if not q:
        return jsonify({
            'success': False,
            'message': "Not updated."
        })

    return jsonify({
        'success': True,
        'message': "Updated."
    })

@app.route('/<db_name>/<table_name>/', methods=['POST'])
def create_object(db_name, table_name):

    obj = request.get_json()

    if '_id' in obj:
        del obj['_id']

    now = datetime.datetime.now()

    obj['createdDate'] = now
    obj['modifiedDate'] = now

    try:
        insert_res = get_db(db_name)[table_name].insert_one(obj)
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

    return dumps({
        'success': True,
        'message': str(insert_res.inserted_id)
    })
    
@app.route('/<db_name>/<table_name>/<object_id>', methods=['DELETE'])
def delete_object(db_name, table_name, object_id):

    t = get_db(db_name)[table_name]
    t.delete_one({ '_id': ObjectId(object_id) })

    return jsonify({
        'success': True
    })