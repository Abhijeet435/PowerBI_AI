import yaml
import os

def load_schema(yml_path: str) -> str:
    abs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), yml_path)
    
    with open(abs_path, 'r') as f:
        schema = yaml.safe_load(f)
    
    # Debug — print raw loaded data
    print("Raw YAML loaded successfully")
    print("Number of tables:", len(schema['tables']))
    
    schema_lines = []

    for table in schema['tables']:
        table_name = table['name']
        db = table.get('database', '')
        sc = table.get('schema', '')

        columns = ', '.join([col['name'] for col in table.get('columns', [])])

        # Safely handle relationships
        relationships = table.get('relationships', [])
        join_parts = []
        for r in relationships:
            join_name = r.get('join', '')
            join_on = r.get('on', '')
            if join_name and join_on:
                join_parts.append(f"{join_name} on {join_on}")

        joins_text = f" | Joins: {', '.join(join_parts)}" if join_parts else ""

        line = f"Table: {db}.{sc}.{table_name} | Columns: {columns}{joins_text}"
        schema_lines.append(line)

    return '\n'.join(schema_lines)


if __name__ == '__main__':
    # Debug — print raw YAML first
    abs_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'AdventureWorks.yml')
    with open(abs_path, 'r') as f:
        raw = yaml.safe_load(f)
    
    # Print relationships of first table to see exact structure
    print("First table relationships raw data:")
    print(raw['tables'][0].get('relationships', []))
    print("---")

    schema = load_schema('AdventureWorks.yml')
    print(schema)
    print("\nTotal tables found:", schema.count('Table:'))