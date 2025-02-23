from pathlib import Path

# Diretório base dos dados
BASE_DIR = Path(r'C:\Users\thiag\desenvolvimentos\pjtCensoEscolar')
RAW_DIR = BASE_DIR / 'data' / 'raw'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'
FINAL_DIR = BASE_DIR / 'data' / 'final'

for directory in [PROCESSED_DIR, FINAL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

FILE_ENCODING = 'latin-1'
CSV_SEPARATOR = '|'

def get_csv_files():
    """
    Encontra todos os arquivos CSV no diretório raw
    """
    csv_files = {}
    for file_path in RAW_DIR.glob('**/*.CSV'):  
        file_key = file_path.stem.lower()  
        csv_files[file_key] = file_path
    return csv_files


CSV_FILES = get_csv_files()

print("\nArquivos CSV encontrados:")
for key, path in CSV_FILES.items():
    print(f"- {key}: {path}")