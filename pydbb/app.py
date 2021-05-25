
import json
import os

from bson.json_util import dumps, loads

from dotenv import load_dotenv
from flask import Flask, jsonify, request

load_dotenv()

from pydbb.fields import get_fields
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
    print(res)

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
    return jsonify({ 'success': True })