
ILLEGAL_CHARACTERS = [' ', ':', "'", '"']

def validate_table_name(n):
    for c in ILLEGAL_CHARACTERS:
        if c in n:
            return (False, f"No '{c}' allowed in table name '{n}'.")
    return (True, "ok")

def create_indices(table, fields):
    for f in fields:
        if 'unique' in fields[f] and fields[f]['unique']:
            table.create_index(f, unique=True)
        if 'text' in fields[f] and fields[f]['text']:
            table.create_index({ f: "text" })