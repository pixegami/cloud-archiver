class ArchiveItem:
    def __init__(self, key: str, path: str):
        self.key: str = key
        self.path: str = path

    def __repr__(self):
        return f"[ArchiveItem: {self.key} path={self.path}]"
