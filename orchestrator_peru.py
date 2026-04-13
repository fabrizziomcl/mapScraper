"""
Massive Geographic Orchestrator for Google Maps Scraper.
Automates country-wide data extraction by traversing hierarchical geographical boundaries.
Features auto-resume capabilities to prevent data loss upon interruption/rate-limits.
"""

import os
import csv
import sys
import argparse
import json

# Ensure internal modules can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.constant import categories
import mapScraper.placesCrawlerV2 as crawler

def sanitize_filename(name):
    """
    Sanitize directory names to prevent path errors across OS.
    Removes invalid characters and trims whitespaces.
    """
    keepcharacters = (' ', '.', '_', '-')
    sanitized = "".join(c for c in name if c.isalnum() or c in keepcharacters).rstrip()
    return sanitized.replace(' ', '_')

def main():
    parser = argparse.ArgumentParser(description="Massive District-level Orchestrator for Peru")
    parser.add_argument('--limit', type=int, default=None, help='Max results per category query (Default: No limit)')
    parser.add_argument('--concurrent', type=int, default=3, help='Concurrent requests limit (Recommended: 3)')
    parser.add_argument('--lang', type=str, default='es', help='Language for results (Default: es)')
    parser.add_argument('--country', type=str, default='pe', help='Target country ISO code (Default: pe)')
    parser.add_argument('--start-idx', type=int, default=0, help='Start index for the district list (0 to 1873)')
    parser.add_argument('--deps', type=json.loads, default=None, help='Filter execution by departments via JSON Array. Ex: --deps "[\"Lima\", \"Cusco\"]"')
    
    args = parser.parse_args()

    input_csv = 'config/geo_ref_pe.csv'
    base_output_dir = 'data'
    
    if not os.path.exists(input_csv):
        print(f"Critical Error: Base geographical CSV '{input_csv}' not found.")
        sys.exit(1)

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = list(csv.DictReader(f))
        
    total_districts = len(reader)
    
    print(f"=== ORCHESTRATOR STARTED ===")
    print(f"Total Target Districts: {total_districts}")
    print(f"Categories per District: {len(categories)}")
    print(f"============================")
    
    for i, row in enumerate(reader):
        if i < args.start_idx:
            continue
            
        dep = row['departamento']
        prov = row['provincia']
        dist = row['distrito']
        
        # Apply department filters if specified via arguments
        if args.deps and dep not in args.deps:
            continue
        
        # Sanitize variables for safe filepath generation
        norm_dep = sanitize_filename(dep)
        norm_prov = sanitize_filename(prov)
        norm_dist = sanitize_filename(dist)
        
        # Construct hierarchical export path: data/Lima/Lima/Miraflores.csv
        out_dir = os.path.join(base_output_dir, norm_dep, norm_prov)
        out_path = os.path.join(out_dir, f"{norm_dist}.csv")
        
        # Auto-resume mechanism: Skip district if CSV already exists
        if os.path.exists(out_path):
            print(f"[{i+1}/{total_districts}] ⏭️ SKIPPING: {norm_dist}.csv already exists in {norm_dep}/{norm_prov}")
            continue
            
        os.makedirs(out_dir, exist_ok=True)
        
        print(f"\n[{i+1}/{total_districts}] 🚀 PROCESSING: {dist}, {prov}, ({dep})")
        
        # Generate target queries in-memory for the current district block
        queries = [f"{cat} en el distrito de {dist}, {prov}, {dep}, Perú" for cat in categories]
        
        # Trigger the core crawler mechanism
        results = crawler.search_multiple(
            queries, 
            lang=args.lang, 
            country=args.country, 
            limit=args.limit, 
            max_concurrent=args.concurrent
        )
        
        if results:
            crawler.save_to_csv(results, out_path)
        else:
            # Handle edge cases where remote/empty districts yield no business records
            print("No results found across all categories for this district.")
            
            # Create a blank structural CSV to ensure the resume-mechanism recognizes it was processed
            with open(out_path, 'w', encoding='utf-8') as empty: 
                empty.write("id,url_place,title,category,address,phoneNumber,completePhoneNumber,domain,url,coor,stars,reviews,source_query\n")

if __name__ == "__main__":
    main()
