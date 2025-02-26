class ETLError(Exception):
    """Classe base para exceções do ETL"""
    pass

class ExtractionError(ETLError):
    """Exceção para erros durante a extração"""
    pass

class TransformationError(ETLError):
    """Exceção para erros durante a transformação"""
    pass

class LoadError(ETLError):
    """Exceção para erros durante o carregamento"""
    pass 