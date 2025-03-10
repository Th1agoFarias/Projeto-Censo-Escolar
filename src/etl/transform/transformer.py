import pandas as pd
import logging
from typing import List, Optional
from .mappings.mapping_utils import MappingManager

class CensoTransformer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.mapping_manager = MappingManager()

    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte as colunas de data para o formato dd/mm/yyyy, lidando com diferentes formatos.
        """
        try:
            date_columns = ['DT_ANO_LETIVO_INICIO', 'DT_ANO_LETIVO_TERMINO']

            for col in date_columns:
                if col in df.columns:
                    self.logger.debug(f"Valores originais em {col}: {df[col].head()}")

                    # Normaliza os valores para evitar espaços extras
                    df[col] = df[col].astype(str).str.strip()

                    # Primeiro tenta converter com ano de 4 dígitos
                    df[col] = pd.to_datetime(df[col], format='%d%b%Y:%H:%M:%S', errors='coerce')
                    
                    # Para valores que não converteram, tenta com ano de 2 dígitos
                    mask_null = df[col].isna()
                    if mask_null.any():
                        temp_dates = pd.to_datetime(
                            df[col][mask_null], 
                            format='%d%b%y:%H:%M:%S', 
                            errors='coerce'
                        )
                        df.loc[mask_null, col] = temp_dates

                    # Converte para o formato brasileiro
                    df[col] = df[col].dt.strftime('%d/%m/%Y')

                    self.logger.debug(f"Valores convertidos em {col}: {df[col].head()}")
                    
                    # Log de valores não convertidos
                    null_count = df[col].isna().sum()
                    if null_count > 0:
                        self.logger.warning(f"{null_count} valores não convertidos na coluna {col}")

            return df

        except Exception as e:
            self.logger.error(f"Erro ao converter datas: {str(e)}")
            raise

    def _apply_mappings(
        self, df: pd.DataFrame, columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """
        Aplica mapeamentos predefinidos às colunas especificadas.
        """
        try:
            df_transformed = df.copy()

            # Adiciona log para debug
            self.logger.debug(f"Colunas disponíveis para transformação: {df.columns.tolist()}")
            self.logger.debug(f"Mapeamentos disponíveis: {list(self.mapping_manager.mappings.keys())}")

            columns_to_transform = columns or [
                col for col in df.columns if col in self.mapping_manager.mappings
            ]

            # Adiciona log das colunas que serão transformadas
            self.logger.info(f"Colunas que serão transformadas: {columns_to_transform}")

            for column in columns_to_transform:
                mapping = self.mapping_manager.get_mapping(column)
                if mapping:
                    # Adiciona log antes da transformação
                    self.logger.debug(f"Valores únicos antes da transformação em {column}: {df_transformed[column].unique()}")

                    df_transformed[column] = df_transformed[column].astype(str)
                    df_transformed[column] = df_transformed[column].map(mapping)

                    # Adiciona log após a transformação
                    self.logger.debug(f"Valores únicos após a transformação em {column}: {df_transformed[column].unique()}")

                    unmapped = df_transformed[df_transformed[column].isna()][column].unique()
                    if len(unmapped) > 0:
                        self.logger.warning(f"Valores não mapeados na coluna {column}: {unmapped}")

            return df_transformed

        except Exception as e:
            self.logger.error(f"Erro ao aplicar mapeamentos: {str(e)}")
            raise

    def transform_data(self, df: pd.DataFrame, year: str = None) -> pd.DataFrame:
        """
        Realiza a transformação dos dados aplicando mapeamentos e conversão de datas.
        """
        try:
            self.logger.info(f"Iniciando transformação dos dados. Shape inicial: {df.shape}")

            # Verifica se o DataFrame precisa ser separado
            if df.shape[1] == 1:
                primeira_coluna = df.columns[0]

                # Verifica se os dados estão em uma única string separada por ";"
                if isinstance(df.iloc[0, 0], str) and ";" in df.iloc[0, 0]:
                    df = pd.DataFrame(
                        [row.split(";") for row in df[primeira_coluna]],
                        columns=primeira_coluna.split(";"),
                    )

                    # Remove espaços em branco dos nomes das colunas
                    df.columns = df.columns.str.strip()

                    self.logger.info(f"DataFrame separado em colunas. Novo shape: {df.shape}")
                    self.logger.info(f"Colunas após separação: {df.columns.tolist()}")

            # Verifica se há dados para transformar
            if df.empty:
                raise ValueError("DataFrame está vazio")

            # Verifica se os mapeamentos foram carregados
            if not self.mapping_manager.mappings:
                raise ValueError("Nenhum mapeamento foi carregado")

            # Log das colunas disponíveis para mapeamento
            available_mappings = list(self.mapping_manager.mappings.keys())
            self.logger.info(f"Mapeamentos disponíveis: {available_mappings}")

            # Identifica as colunas que podem ser transformadas
            columns_to_transform = [col for col in df.columns if col in available_mappings]
            self.logger.info(f"Colunas que serão transformadas: {columns_to_transform}")

            if not columns_to_transform:
                raise ValueError("Nenhuma coluna para transformar foi encontrada")

            # Remove linhas com valores ausentes apenas nas colunas que serão transformadas
            df_clean = df.dropna(subset=columns_to_transform)
            self.logger.info(f"Shape após limpeza: {df_clean.shape}")

            # Aplica os mapeamentos
            df_transformed = self._apply_mappings(df_clean, columns_to_transform)

            # Aplica a conversão de datas se um ano for fornecido
            if year:
                df_transformed = self._convert_dates(df_transformed)
                self.logger.info("Conversão de datas concluída")

            self.logger.info(f"Transformação concluída. Shape final: {df_transformed.shape}")

            return df_transformed

        except Exception as e:
            self.logger.error(f"Erro na transformação: {str(e)}")
            raise

    def validate_transformation(self, df: pd.DataFrame) -> bool:
        """
        Valida os dados transformados.
        """
        try:
            # Implementar validações específicas aqui
            return True
        except Exception as e:
            self.logger.error(f"Erro na validação: {str(e)}")
            return False
