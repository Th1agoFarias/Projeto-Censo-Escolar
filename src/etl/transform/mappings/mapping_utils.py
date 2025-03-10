import json
import os
import logging
from typing import Dict, Any, Optional
import pandas as pd

class MappingManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.mappings: Dict[str, Dict[str, Any]] = {}
        self._load_mappings()

    def _load_mappings(self) -> None:
        """Carrega os mapeamentos do arquivo maps.json"""
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            json_path = os.path.join(current_dir, 'maps.json')
            
            self.logger.info(f"Tentando carregar mapeamentos de: {json_path}")
            
            with open(json_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                
            # Extrai os mapeamentos da estrutura aninhada
            if 'mappings' in data:
                for column, mapping_info in data['mappings'].items():
                    if 'values' in mapping_info:
                        self.mappings[column] = mapping_info['values']
            
            self.logger.info(f"Mapeamentos carregados com sucesso. Colunas disponíveis: {list(self.mappings.keys())}")
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar mapeamentos: {str(e)}")
            raise

    def get_mapping(self, column_name: str) -> Optional[Dict[str, Any]]:
        """Retorna o mapeamento para uma coluna específica"""
        try:
            if column_name in self.mappings:
                mapping = self.mappings[column_name]
                self.logger.debug(f"Mapeamento encontrado para coluna {column_name}")
                return mapping
            return None
        except Exception as e:
            self.logger.error(f"Erro ao buscar mapeamento para {column_name}: {str(e)}")
            return None

    def add_mapping(self, column_name: str, mapping: Dict[str, Any]) -> None:
        """
        Adiciona ou atualiza um mapeamento
        
        Args:
            column_name: Nome da coluna
            mapping: Dicionário com o mapeamento
        """
        try:
            self.mappings[column_name] = mapping
            self.logger.info(f"Mapeamento adicionado/atualizado para coluna: {column_name}")
        except Exception as e:
            self.logger.error(f"Erro ao adicionar mapeamento para {column_name}: {str(e)}")
            raise

    def remove_mapping(self, column_name: str) -> None:
        """
        Remove um mapeamento
        
        Args:
            column_name: Nome da coluna
        """
        try:
            if column_name in self.mappings:
                del self.mappings[column_name]
                self.logger.info(f"Mapeamento removido para coluna: {column_name}")
            else:
                self.logger.warning(f"Tentativa de remover mapeamento inexistente: {column_name}")
        except Exception as e:
            self.logger.error(f"Erro ao remover mapeamento para {column_name}: {str(e)}")
            raise

    def map_column(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """Aplica o mapeamento da coluna no dataframe."""
        if column_name in self.mappings:
            mapping = self.mappings[column_name]
            df[column_name] = df[column_name].map(mapping).fillna(df[column_name])
        return df

    def map_columns(self, df: pd.DataFrame, column_names: list) -> pd.DataFrame:
        """Aplica o mapeamento de múltiplas colunas."""
        for column in column_names:
            df = self.map_column(df, column)
        return df

