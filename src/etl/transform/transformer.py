import pandas as pd
import logging
from typing import List, Optional
from .mappings.mapping_utils import MappingManager

class CensoTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.mapping_manager = MappingManager()

    def _apply_mappings(self, 
                       df: pd.DataFrame, 
                       columns: Optional[List[str]] = None) -> pd.DataFrame:
      
        try:
            df_transformed = df.copy()
            
            # Se columns não for especificado, usa todas as colunas com mapeamento
            columns_to_transform = columns or [col for col in df.columns 
                                            if col in self.mapping_manager.mappings]

            for column in columns_to_transform:
                mapping = self.mapping_manager.get_mapping(column)
                if mapping:
                    df_transformed[column] = df_transformed[column].astype(str)
                    df_transformed[column] = df_transformed[column].map(mapping)
                    
                    unmapped = df_transformed[df_transformed[column].isna()][column].unique()
                    if len(unmapped) > 0:
                        self.logger.warning(
                            f"Valores não mapeados na coluna {column}: {unmapped}"
                        )

            return df_transformed

        except Exception as e:
            self.logger.error(f"Erro ao aplicar mapeamentos: {str(e)}")
            raise

    def transform_data(self, 
                      df: pd.DataFrame, 
                      columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Transforma os dados aplicando os mapeamentos necessários.
        
        Args:
            df: DataFrame a ser transformado
            columns: Lista opcional de colunas específicas para transformar
        """
        try:
            self.logger.info("Iniciando transformação dos dados")
            
            # Remove linhas com valores ausentes
            df_clean = df.dropna(subset=columns if columns else df.columns)
            
            # Aplica os mapeamentos
            df_transformed = self._apply_mappings(df_clean, columns)
            
            self.logger.info("Transformação concluída com sucesso")
            return df_transformed

        except Exception as e:
            self.logger.error(f"Erro na transformação: {str(e)}")
            raise

    def validate_transformation(self, df: pd.DataFrame) -> bool:
        """Valida os dados transformados."""
        try:
            # Implementar validações específicas aqui
            return True
        except Exception as e:
            self.logger.error(f"Erro na validação: {str(e)}")
            return False
