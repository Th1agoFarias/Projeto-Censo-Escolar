import pandas as pd
import logging
from pathlib import Path
from config import PROCESSED_DIR

class CensoTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def transform_data(self, file_type: str, df: pd.DataFrame) -> pd.DataFrame:
        """Aplica transformações nos dados e salva o resultado."""
        try:
            # Exemplo de transformação (pode ser ajustado conforme necessário)
            df_transformed = df.dropna()  # Remove valores nulos

            output_path = Path(PROCESSED_DIR) / f"{file_type}_processed.csv"
            df_transformed.to_csv(output_path, index=False)

            self.logger.info(f"Dados de {file_type} transformados com sucesso e salvos em {output_path}")
            return df_transformed

        except Exception as e:
            self.logger.error(f"Erro na transformação de {file_type}: {str(e)}")
            raise
