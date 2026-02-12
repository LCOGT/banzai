class InhomogeneousSetException(Exception):
    pass


class MissingCatalogException(Exception):
    pass


class FrameNotAvailableError(Exception):
    """Raised when a frame cannot be downloaded (NULL frameid or not in archive)"""
    pass
