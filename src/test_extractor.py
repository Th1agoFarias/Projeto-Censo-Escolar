import pytest
import tempfile
from pathlib import Path
from src.extractor import CensoExtractor
from src.config import CSV_FILES

def test_extractor_basic_functionality():
    """Teste básico do CensoExtractor"""
    # Cria arquivo CSV temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix='.CSV', mode='w', encoding='latin-1') as temp_file:
        temp_file.write("coluna1|coluna2\n")
        temp_file.write("valor1|valor2\n")
    
    try:
        # Configura o arquivo para teste
        temp_path = Path(temp_file.name)
        CSV_FILES['teste'] = temp_path

        # Testa o extractor
        extractor = CensoExtractor()
        assert extractor.validate_files(), "Arquivo não encontrado"
        df = extractor.read_data('teste')
        assert not df.empty, "DataFrame vazio"

    finally:
        # Limpa
        temp_path.unlink(missing_ok=True)
        CSV_FILES.clear()