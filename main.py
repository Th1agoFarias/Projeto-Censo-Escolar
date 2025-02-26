import logging
import logging.config
from pathlib import Path
from src.config.settings import LOG_CONFIG, RAW_DATA_DIR, PROCESSED_DATA_DIR
from src.pipeline.etl_pipeline import ETLPipeline

def main():
    # Configura logging
    logging.config.dictConfig(LOG_CONFIG)
    logger = logging.getLogger(__name__)

    try:
        # Define caminhos
        input_file = RAW_DATA_DIR / "input.csv"
        output_file = PROCESSED_DATA_DIR / "output.csv"

        # Executa pipeline
        pipeline = ETLPipeline()
        success = pipeline.run(input_file, output_file)

        if success:
            logger.info("Processo ETL concluído com sucesso")
        else:
            logger.error("Processo ETL falhou")

    except Exception as e:
        logger.error(f"Erro na execução do programa: {str(e)}")
        raise

if __name__ == "__main__":
    main() 