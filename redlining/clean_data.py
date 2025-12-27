#!/usr/bin/env python3
"""
Clean redlining data:
- Standardize 'grade' column name (remove spaces)
- Clean grade values (trim whitespace, convert empty to null)
"""

import argparse
import geopandas as gpd
import pandas as pd

def clean_grade_column(gdf):
    """Clean the grade column"""
    # Find the grade column (might be 'grade' or 'grade ' or 'Grade' etc)
    grade_col = None
    for col in gdf.columns:
        if col.strip().lower() == 'grade':
            grade_col = col
            break
    
    if grade_col is None:
        print("Warning: No 'grade' column found")
        return gdf
    
    print(f"Found grade column: '{grade_col}'")
    
    # Rename column to standardized name if needed
    if grade_col != 'grade':
        gdf = gdf.rename(columns={grade_col: 'grade'})
        print(f"Renamed '{grade_col}' to 'grade'")
    
    # Clean the values
    # Strip whitespace from all string values
    gdf['grade'] = gdf['grade'].astype(str).str.strip()
    
    # Convert empty strings and 'nan' to None/null
    gdf['grade'] = gdf['grade'].replace(['', 'nan', 'None'], None)
    
    # Show value counts
    print("\nGrade value distribution after cleaning:")
    print(gdf['grade'].value_counts(dropna=False))
    
    return gdf


def main():
    parser = argparse.ArgumentParser(description="Clean redlining data")
    parser.add_argument("--input", required=True, help="Input geopackage file")
    parser.add_argument("--output", required=True, help="Output parquet file")
    args = parser.parse_args()
    
    print(f"Reading {args.input}...")
    gdf = gpd.read_file(args.input)
    
    print(f"Original shape: {gdf.shape}")
    print(f"Original columns: {list(gdf.columns)}")
    
    # Clean the grade column
    gdf = clean_grade_column(gdf)
    
    print(f"\nWriting cleaned data to {args.output}...")
    gdf.to_parquet(args.output)
    
    print("✓ Cleaning complete!")


if __name__ == "__main__":
    main()
