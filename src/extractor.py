import pandas as pd 
import logging
from src.config import CSV_FILES, FILE_ENCODING, CSV_SEPARATOR
from pathlib import Path

class CensoExtractor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.dataframes = {}

    def validate_files(self) -> bool:
        """
        Valida se os arquivos existem 
        """
        if len(CSV_FILES) == 0:  # Modificado aqui
            self.logger.error("Nenhum arquivo CSV encontrado")
            return False
        
        for file_type, file_path in CSV_FILES.items():
            if not Path(file_path).exists():  # Modificado aqui
                self.logger.error(f"Arquivo {file_type} não encontrado: {file_path}")
                return False
        return True
    
    def read_data(self, file_type: str) -> pd.DataFrame:
        """
        Lê um arquivo específico do Censo
        """
        try: 
            file_path = CSV_FILES[file_type]
            df = pd.read_csv(
                file_path,
                encoding=FILE_ENCODING,
                sep=CSV_SEPARATOR,
                low_memory=False
            )
            self.logger.info(f"Arquivo {file_type} lido com sucesso. Shape: {df.shape}")
            return df
        except Exception as e: 
            self.logger.error(f"Erro ao ler arquivo {file_type}: {str(e)}")
            raise