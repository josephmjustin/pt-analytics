"""
Extract nested zip files from BODS TransXChange download
Handles: main.zip -> operator_zips/ -> xml_files/
"""

import zipfile
import os
from pathlib import Path

# Configuration
MAIN_ZIP = "C:/Users/justi/Work/Personal/pt-analytics/static/transxchange_downloads/compresed.zip"  # Update this path
OUTPUT_DIR = "C:/Users/justi/Work/Personal/pt-analytics/static/transxchange_downloads"
TEMP_DIR = "C:/Users/justi/Work/Personal/pt-analytics/static/temp_zips"

def extract_nested_zips(main_zip_path, output_dir, temp_dir):
    """Extract all XML files from nested zip structure"""
    
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(temp_dir, exist_ok=True)
    
    print(f"Extracting from: {main_zip_path}")
    print(f"Output directory: {output_dir}")
    print("="*80)
    
    # Step 1: Extract main zip
    print("\nStep 1: Extracting main zip file...")
    with zipfile.ZipFile(main_zip_path, 'r') as main_zip:
        main_zip.extractall(temp_dir)
    print(f"  ✓ Extracted to {temp_dir}")
    
    # Step 2: Find and extract nested zips
    print("\nStep 2: Finding nested zip files...")
    nested_zips = list(Path(temp_dir).rglob("*.zip"))
    print(f"  Found {len(nested_zips)} operator zip files")
    
    xml_count = 0
    
    for i, zip_path in enumerate(nested_zips, 1):
        operator_name = zip_path.stem
        print(f"\n  [{i}/{len(nested_zips)}] Processing: {operator_name}")
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as nested_zip:
                # Extract only XML files
                xml_files = [f for f in nested_zip.namelist() if f.lower().endswith('.xml')]
                
                for xml_file in xml_files:
                    # Extract to output directory
                    nested_zip.extract(xml_file, output_dir)
                    xml_count += 1
                
                print(f"    ✓ Extracted {len(xml_files)} XML files")
        
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    print("\n" + "="*80)
    print(f"EXTRACTION COMPLETE")
    print(f"  Total XML files extracted: {xml_count}")
    print(f"  Location: {output_dir}")
    print("="*80)
    
    # Cleanup temp directory
    print("\nCleaning up temporary files...")
    import shutil
    shutil.rmtree(temp_dir)
    print("  ✓ Cleanup complete")

if __name__ == "__main__":
    extract_nested_zips(MAIN_ZIP, OUTPUT_DIR, TEMP_DIR)