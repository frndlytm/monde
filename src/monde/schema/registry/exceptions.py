class RegistryKeyError(KeyError):
    def __init__(self, key: str):
        super().__init__(key)
