
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

def convert_record(schema, record):

    out = {}

    out['_id'] = str(record['_id'])

    for field_name in schema:
        if field_name in record:
            if schema[field_name]['dataType'] == 'date':
                out[field_name] = record[field_name].strftime('%Y-%m-%dT%H:%M:%S')
            else:
                out[field_name] = record[field_name]
    
    return out