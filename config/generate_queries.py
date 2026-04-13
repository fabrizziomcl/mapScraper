"""
Massive Query Generator Utility.
Programmatically builds a flat text file of all possible search queries by cross-referencing
business categories with Peruvian districts.
Note: Superseded by the on-the-fly approach in orchestrator_peru.py, but kept for legacy usage.
"""

import csv
from constant import categories

# I/O setup
INPUT_CSV = 'geo_ref_pe.csv'
OUTPUT_TXT = 'consultas_masivas_peru.txt'

def generate_queries():
    """Reads categories and local geographical data to output precomputed search queries."""
    print(f"Loaded {len(categories)} categories from constant.py")

    total_queries = 0
    with open(INPUT_CSV, 'r', encoding='utf-8') as f_csv, open(OUTPUT_TXT, 'w', encoding='utf-8') as f_txt:
        reader = csv.DictReader(f_csv)
        
        # Load districts into memory to speed up multi-iteration crossing
        districts = list(reader)
        print(f"Loaded {len(districts)} districts from {INPUT_CSV}")
        
        for cat in categories:
            for row in districts:
                dep = row['departamento']
                prov = row['provincia']
                dist = row['distrito']
                
                # Format specific queries (appending 'Perú' to force precise geocoding from Google Maps)
                query = f"{cat} en el distrito de {dist}, {prov}, {dep}, Perú\n"
                f_txt.write(query)
                total_queries += 1

    print(f"Finished! Generated {total_queries} queries and saved to '{OUTPUT_TXT}'.")

if __name__ == "__main__":
    generate_queries()
