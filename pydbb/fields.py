
def get_fields(schema):

    fields = {
        '_id': { 'read_only': True },
        'createdDate': { 'read_only': True },
        'modifiedDate': { 'read_only': True }
    }

    if 'fields' in schema:
        
        for f in fields:
            if f in schema['fields']:
                return jsonify({
                    'success': False,
                    'message': f"Cannot define field '{f}'."
                })

        fields = { **fields, **(schema['fields']) }