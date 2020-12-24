class ArchivePath:
    def __init__(self, path: str, days_since_access: int, should_archive: bool,
                 is_root: bool, is_dir: bool, is_ignored: bool):
        self.key: str = path
        self.days_since_access: int = days_since_access
        self.is_root: bool = is_root
        self.is_dir: bool = is_dir
        self.ignored: bool = is_ignored
        self.should_archive: bool = not self.ignored and should_archive

    def __repr__(self):
        return f"[ArchivePath: {self.key} " \
               f"days={self.days_since_access} " \
               f"should_archive={self.should_archive} " \
               f"root={self.is_root}]"
