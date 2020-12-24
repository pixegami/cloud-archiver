class ArchiveKey:
    def __init__(self, key: str, path: str):
        self.key: str = key
        self.path: str = path

    def __repr__(self):
        return f"[ArchiveKey: {self.key} path={self.path}]"
