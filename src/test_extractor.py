import tempfile
import pandas as pd
import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.etl.extract.extractor import DataExtractor
from src.config.settings import CSV_FILES, FILE_ENCODING, CSV_SEPARATOR
from src.utils.exceptions import ExtractionError

class TestDataExtractor:
    def setup_method(self):
        """Setup para cada teste"""
        self.extractor = DataExtractor()

    def test_extractor_basic_functionality(self):
        """Teste básico do DataExtractor"""
        # Cria arquivo CSV temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix='.CSV', mode='w', encoding=FILE_ENCODING) as temp_file:
            temp_file.write(f"coluna1{CSV_SEPARATOR}coluna2\n")
            temp_file.write(f"valor1{CSV_SEPARATOR}valor2\n")
        
        try:
            # Configura o arquivo para teste
            temp_path = Path(temp_file.name)
            CSV_FILES['teste'] = temp_path

            # Testa a extração
            df = self.extractor.extract_from_csv(temp_path)
            
            # Validações
            assert isinstance(df, pd.DataFrame), "Resultado não é um DataFrame"
            assert not df.empty, "DataFrame está vazio"
            assert df.shape == (1, 2), "DataFrame não tem o formato esperado"
            assert list(df.columns) == ['coluna1', 'coluna2'], "Colunas não correspondem"
            assert df.iloc[0]['coluna1'] == 'valor1', "Valor não corresponde"
            assert df.iloc[0]['coluna2'] == 'valor2', "Valor não corresponde"

        finally:
            # Limpa
            temp_path.unlink(missing_ok=True)
            if 'teste' in CSV_FILES:
                del CSV_FILES['teste']