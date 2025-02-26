import logging
from pathlib import Path
from src.etl.extract.extractor import DataExtractor
from src.etl.transform.transformer import DataTransformer
from src.etl.load.loader import DataLoader
from src.utils.exceptions import ETLError

class ETLPipeline:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()

    def run(self, input_path: Path, output_path: Path) -> bool:
        """Executa o pipeline ETL completo."""
        try:
            # Extract
            self.logger.info("Iniciando etapa de extração")
            df = self.extractor.extract_from_csv(input_path)
            if not self.extractor.validate_data(df):
                raise ETLError("Falha na validação dos dados extraídos")

            # Transform
            self.logger.info("Iniciando etapa de transformação")
            df_transformed = self.transformer.transform_data(df)
            if not self.transformer.validate_transformation(df_transformed):
                raise ETLError("Falha na validação da transformação")

            # Load
            self.logger.info("Iniciando etapa de carregamento")
            self.loader.load_to_csv(df_transformed, output_path)
            if not self.loader.validate_load(output_path):
                raise ETLError("Falha na validação do carregamento")

            self.logger.info("Pipeline ETL executado com sucesso")
            return True

        except Exception as e:
            self.logger.error(f"Erro no pipeline ETL: {str(e)}")
            return False 