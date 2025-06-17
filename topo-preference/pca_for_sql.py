import sqlite3, pandas as pd, tensorflow as tf, numpy as np, os, argparse, fnmatch 

'''
Usage: 
python pca_tool.py  
    --db data/{client}.db  
    --table table_name  
    --N 5 \
    --static [col1, col2]
'''

def run_pca(X: tf.Tensor, n: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    '''
    Centers some information in tensor X and 
    returns the projection of X onto the principal axes
    '''
    mean = tf.reduce_mean(X, axis=0, keepdims=True)
    centered = X - mean 

    s, _, v = tf.linalg.svd(centered, full_matrices=False)

    principal_axes = tf.transpose(v)[:, :n]
    scores = tf.matmul(centered, principal_axes)
    var    = (s ** 2) / tf.reduce_sum(s ** 2)
    return scores.numpy(), principal_axes.numpy(), var[:n].numpy()

def main():
    p = argparse.ArgumentParser(description="PCA Reduction on SQLite table")
    p.add_argument("--db", required=True)
    p.add_argument("--table", required=True)
    p.add_argument("--N", type=int, default=5, help="# of principal axes")
    p.add_argument("--static", nargs="*", required=True)
    
    # Get args 
    args    = p.parse_args()
    db_path = args.db 
    table   = args.table 

    with sqlite3.connect("data/noaa.db") as con:
        cols = pd.read_sql("PRAGMA table_info(state_climate_raw);", con)["name"].tolist()
        print(cols[:80])
    
    # Get table 
    with sqlite3.connect(db_path) as dat:
        df = pd.read_sql(f"SELECT * FROM {table}", dat)

    # Allow pattern matching from tables 
    static_patterns = args.static 
    static_cols = []
    for pattern in static_patterns: 
        if pattern in df.columns: 
            static_cols.append(pattern)
        else: 
            static_cols += fnmatch.filter(df.columns, pattern)
    static_cols = list(dict.fromkeys(static_cols))

    # Get static and numeric dataframes 
    static_df  = df[static_cols].reset_index(drop=True)
    numeric_df = (df.select_dtypes(include=[np.number])
                    .drop(columns=[c for c in static_cols if c in df.select_dtypes(include=[np.number]).columns], errors='ignore')) 
    
    # Expect nonempty 
    if numeric_df.empty: 
        raise RuntimeError("No numeric columns exist")

    # Impute means for NaN 
    numeric_df = numeric_df.apply(lambda col: col.fillna(col.mean()))

    # Get values from pca
    scores, weights, var = run_pca(numeric_df.values, n=args.N) #type: ignore 
    cols = [f"PC{i+1}" for i in range(args.N)]

    # PCA resulting dataset 
    pca_df  = pd.concat([static_df, pd.DataFrame(scores, columns=pd.Index(cols))], axis=1)
    # Weight each column contributes to retained principal component 
    weight_df = (pd.DataFrame(weights, columns=pd.Index(cols),index=numeric_df.columns)
               .reset_index()
               .rename(columns={"index": "feature"}))
    # Fraction of total variance a principal component captures (eigenvalue)
    var_df  = pd.DataFrame({"PC": cols, "variance": var})
    
    # Get new database string and save 
    base, _ = os.path.splitext(db_path)
    new_path = f"{base}_new.db"

    with sqlite3.connect(new_path) as dat: 
        pca_df.to_sql(table + "_pca", dat, if_exists="replace", index=False)
        weight_df.to_sql(table + "_weights", dat, if_exists="replace", index=False)
        var_df.to_sql(table + "_var", dat, if_exists="replace", index=False)


    print(f"Successfully write {df.shape[0]}x{df.shape[1]} to {new_path}:{table}")
    
if __name__ == "__main__":
    main()
