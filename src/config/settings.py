from pathlib import Path

# Diretório base dos dados
BASE_DIR = Path(r'C:\Users\thiag\desenvolvimentos\pjtCensoEscolar')
RAW_DIR = BASE_DIR / 'data' / 'raw'
PROCESSED_DIR = BASE_DIR / 'data' / 'processed'  # Definição da variável
FINAL_DIR = BASE_DIR / 'data' / 'final'

# Criar diretórios se não existirem
for directory in [PROCESSED_DIR, FINAL_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Configurações de arquivo
FILE_ENCODING = 'latin-1'
CSV_SEPARATOR = '|'

# Função para encontrar arquivos CSV
def get_csv_files():
    """
    Encontra todos os arquivos CSV no diretório raw
    """
    csv_files = {}
    for file_path in RAW_DIR.glob('**/*.CSV'):
        file_key = file_path.stem.lower()  # Usando o nome do arquivo como chave
        csv_files[file_key] = file_path
    return csv_files

# Dicionário com os arquivos CSV encontrados
CSV_FILES = get_csv_files()
