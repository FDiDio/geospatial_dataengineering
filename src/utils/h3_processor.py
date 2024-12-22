import h3

def precompute_h3_index_lookup(df, resolution=5):
    """
    Precompute H3 geospatial indices as a lookup table.

    Args:
        df (pd.DataFrame): DataFrame containing latitude and longitude columns.
        resolution (int): H3 resolution for geospatial indexing.

    Returns:
        pd.DataFrame: A lookup table with unique coordinates and H3 indices.
    """
    print("Precomputing H3 lookup table")
    
    unique_coords = df[['latitude', 'longitude']].drop_duplicates()
    unique_coords['h3_index'] = unique_coords.apply(
        lambda row: h3.latlng_to_cell(row['latitude'], row['longitude'], resolution), axis=1
    )
    return unique_coords

def map_h3_indices_to_dataframe(df, h3_lookup):
    """
    Map precomputed H3 indices to the DataFrame based on latitude and longitude.

    Args:
        df (pd.DataFrame): DataFrame to map H3 indices onto.
        h3_lookup (pd.DataFrame): Lookup table containing precomputed H3 indices.

    Returns:
        pd.DataFrame: DataFrame with H3 indices mapped to it.
    """
    print("Mapping H3 indices to the DataFrame")
    df_with_h3 = df.merge(h3_lookup, on=['latitude', 'longitude'], how='left')
    return df_with_h3
