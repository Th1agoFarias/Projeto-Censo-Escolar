import json
import pandas as pd

class MappingManager:
    def __init__(self, mappings_json=None):
        if mappings_json:
            self.mappings = mappings_json
        else:
            self._load_mappings()

    def _load_mappings(self):
        """Carrega o JSON de mapeamentos de um arquivo externo."""
        with open('src/etl/transform/mappings/maps.json', 'r', encoding='utf-8') as file:
            self.mappings = json.load(file)

    def map_column(self, df, column_name):
        """Aplica o mapeamento da coluna no dataframe."""
        if column_name in self.mappings:
            mapping = self.mappings[column_name]['values']
            df[column_name] = df[column_name].map(mapping).fillna(df[column_name])
        return df

    def map_columns(self, df, column_names):
        """Aplica o mapeamento de múltiplas colunas."""
        for column in column_names:
            df = self.map_column(df, column)
        return df
