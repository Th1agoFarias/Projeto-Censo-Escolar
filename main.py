import pandas as pd
import logging
from pathlib import Path
from src.etl.extract.extractor import CensoExtractor
from src.etl.transform.transformer import CensoTransformer  # Certifique-se de que o caminho está correto
from src.config.settings import CSV_FILES, PROCESSED_DIR, FILE_ENCODING, CSV_SEPARATOR

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    for file_key, file_path in CSV_FILES.items():
        logger.info(f"Processando arquivo: {file_key}")

        try:
            df = pd.read_csv(file_path, encoding=FILE_ENCODING, sep=CSV_SEPARATOR)
            logger.info(f"Arquivo {file_key} carregado com sucesso. Número de linhas: {len(df)}")

            transformer = CensoTransformer()

            df_transformed = transformer.transform_data(df)

            if transformer.validate_transformation(df_transformed):
                output_path = PROCESSED_DIR / f"{file_key}_processed.csv"
                df_transformed.to_csv(output_path, encoding=FILE_ENCODING, sep=CSV_SEPARATOR, index=False)
                logger.info(f"Arquivo transformado salvo em: {output_path}")
            else:
                logger.warning(f"Validação falhou para o arquivo {file_key}. Arquivo não será salvo.")

        except Exception as e:
            logger.error(f"Erro ao processar o arquivo {file_key}: {str(e)}")


if __name__ == "__main__":
    main()
