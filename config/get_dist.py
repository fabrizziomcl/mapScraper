"""
Ubigeo Data Fetcher.
Downloads, processes, and merges Peruvian geographical data (Districts, Provinces, Departments)
from public open datasets to generate structured references for the Orchestrator.
"""

import json
import requests
import pandas as pd

# URLs targeting raw JSON geographical references 
url_dept = "https://raw.githubusercontent.com/ernestorivero/Ubigeo-Peru/master/json/ubigeo_peru_2016_departamentos.json"
url_prov = "https://raw.githubusercontent.com/ernestorivero/Ubigeo-Peru/master/json/ubigeo_peru_2016_provincias.json"
url_dist = "https://raw.githubusercontent.com/ernestorivero/Ubigeo-Peru/master/json/ubigeo_peru_2016_distritos.json"

print("Downloading UBIGEO geographical datasets...")
dept_data = requests.get(url_dept).json()
prov_data = requests.get(url_prov).json()
dist_data = requests.get(url_dist).json()

# Convert datasets to pandas DataFrames and prepare for table merging
df_dept = pd.DataFrame(dept_data).rename(columns={'id': 'department_id', 'name': 'departamento'})
df_prov = pd.DataFrame(prov_data).rename(columns={'id': 'province_id', 'name': 'provincia'})
df_dist = pd.DataFrame(dist_data).rename(columns={'id': 'ubigeo', 'name': 'distrito'})

print("Merging relational tables...")

# Step 1: Link Districts to their respective Provinces
df_merged = pd.merge(df_dist, df_prov[['province_id', 'provincia']], on='province_id', how='left')

# Step 2: Link the resulting dataset to Departments
df_final = pd.merge(df_merged, df_dept[['department_id', 'departamento']], on='department_id', how='left')

# Clean output columns and enforce alphabetical ordering
df_final = df_final[['ubigeo', 'departamento', 'provincia', 'distrito']]
df_final = df_final.sort_values(by=['departamento', 'provincia', 'distrito'])

# Output 1: Save canonical CSV reference dataset
df_final.to_csv('geo_ref_pe.csv', index=False, encoding='utf-8')

# Output 2: Save as hierarchical JSON dictionary
hierarchy = {}
for _, row in df_final.iterrows():
    dep = row['departamento']
    prov = row['provincia']
    dist = row['distrito']
    
    if dep not in hierarchy:
        hierarchy[dep] = {}
    if prov not in hierarchy[dep]:
        hierarchy[dep][prov] = []
        
    hierarchy[dep][prov].append(dist)

with open('diccionario_distritos_peru.json', 'w', encoding='utf-8') as f:
    json.dump(hierarchy, f, ensure_ascii=False, indent=4)

print(f"Success! Processed {len(df_final)} districts.")
print("- Generated geo_ref_pe.csv")
print("- Generated diccionario_distritos_peru.json")