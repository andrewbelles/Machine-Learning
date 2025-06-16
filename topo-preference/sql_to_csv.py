import argparse, sqlite3, sys, pandas as pd 
from pathlib import Path 

'''
standard usage: 
python sql_to_csv.py --db data/{client}.db --table {table_name}

custom query:
python sql_to_csv.py --db data/{client}.db -- query "sql_query"

custom path:
python sql_to_csv.py --db data/{client}.db --table {table_name} --out {table_name}.csv

'''

def main():
    p = argparse. ArgumentParser()
    p.add_argument("--db",    required=True, help="SQLite file")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--table", help="table name to export")
    g.add_argument("--query", default=None, help="custom SQL query")
    p.add_argument("--out",   default=None,  help="output CSV file (default: auto)")
    p.add_argument("--index", action="store_true",
                   help="include row index in CSV")
    
    args = p.parse_args()

    sql_prompt = f"SELECT * FROM {args.table}" if args.table else args.query
    out_file   = args.out 

    if out_file is None: 
        base = Path(args.db).with_suffix("")
        name = (args.table or "query").replace(" ", "-")
        out_file = f"{base}-{name}.csv"

    try: 
        with sqlite3.connect(args.db) as dat: 
            df = pd.read_sql(sql_prompt, dat)
    except Exception as e: 
        sys.exit(f"SQL Error:{e}")

    Path(out_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_file, index=args.index)

if __name__ == "__main__":
    main()
