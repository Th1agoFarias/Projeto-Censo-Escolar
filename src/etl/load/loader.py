import pandas as pd
import logging
from pathlib import Path
from src.utils.exceptions import LoadError

class DataLoader:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def load_to_csv(self, df: pd.DataFrame, output_path: Path) -> None:
        """Carrega os dados para um arquivo CSV."""
        try:
            self.logger.info(f"Salvando dados em: {output_path}")
            df.to_csv(output_path, index=False)
            self.logger.info("Dados salvos com sucesso")
        except Exception as e:
            raise LoadError(f"Erro ao salvar dados: {str(e)}")

    def validate_load(self, output_path: Path) -> bool:
        """Valida se os dados foram carregados corretamente."""
        try:
            return output_path.exists()
        except Exception as e:
            self.logger.error(f"Erro na validação do carregamento: {str(e)}")
            return False 