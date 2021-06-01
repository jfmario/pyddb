
def get_fields(schema):

    fields = {
        'createdDate': { 'read_only': True, 'dataType': "date" },
        'modifiedDate': { 'read_only': True, 'dataType': "date" }
    }

    if 'fields' in schema:
        
        for f in fields:
            if f in schema['fields']:
                return jsonify({
                    'success': False,
                    'message': f"Cannot define field '{f}'."
                })

        fields = { **fields, **(schema['fields']) }

    return fields

def pre_save(schema, record):
    '''Cleans a record before every save.'''

    obj = { **record }

    # set default
    for field_name in schema:
        if field_name not in record:
            if 'default' in schema[field_name]:
                obj[field_name] = schema[field_name]['default']
        else:
            if 'options' in schema[field_name]:
                if record[field_name] not in schema[field_name]['options']:
                    raise ValueError(f"'{record[field_name]}' is not a valid choice for field 'field_name'.")

    # apply other validation

    return obj

def convert_record(schema, record):
    '''Converts a record for JSON output.'''

    out = {}

    out['_id'] = str(record['_id'])

    for field_name in schema:
        if field_name in record:
            if schema[field_name]['dataType'] == 'date':
                out[field_name] = record[field_name].strftime('%Y-%m-%dT%H:%M:%S')
            else:
                out[field_name] = record[field_name]
    
    return out