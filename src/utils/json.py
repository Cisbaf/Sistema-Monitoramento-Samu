from typing import List, Dict


def remove_keys(data: Dict, keys: List[str]) -> Dict:
    """
    Remove uma lista de chaves de um dicionário.

    Args:
        data (Dict): Dicionário de entrada.
        keys (List[str]): Lista de chaves a serem removidas.

    Returns:
        Dict: Novo dicionário sem as chaves removidas.
    """
    return {k: v for k, v in data.items() if k not in keys}