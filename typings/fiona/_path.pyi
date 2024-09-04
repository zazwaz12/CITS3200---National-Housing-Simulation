"""
This type stub file was generated by pyright.
"""

import attr

"""Dataset paths, identifiers, and filenames

Note: this module is not part of Rasterio's API. It is for internal use
only.

"""
SCHEMES = ...
ARCHIVESCHEMES = set
CURLSCHEMES = ...
REMOTESCHEMES = ...

class _Path:
    """Base class for dataset paths"""

    def as_vsi(self):  # -> Any | str:
        ...

@attr.s(slots=True)
class _ParsedPath(_Path):
    """Result of parsing a dataset URI/Path

    Attributes
    ----------
    path : str
        Parsed path. Includes the hostname and query string in the case
        of a URI.
    archive : str
        Parsed archive path.
    scheme : str
        URI scheme such as "https" or "zip+s3".
    """

    path = ...
    archive = ...
    scheme = ...
    @classmethod
    def from_uri(cls, uri):  # -> _ParsedPath:
        ...

    @property
    def name(self):  # -> Any | str:
        """The parsed path's original URI"""
        ...

    @property
    def is_remote(self):  # -> bool:
        """Test if the path is a remote, network URI"""
        ...

    @property
    def is_local(self):  # -> Any | bool:
        """Test if the path is a local URI"""
        ...

@attr.s(slots=True)
class _UnparsedPath(_Path):
    """Encapsulates legacy GDAL filenames

    Attributes
    ----------
    path : str
        The legacy GDAL filename.
    """

    path = ...
    @property
    def name(self):  # -> Any:
        """The unparsed path's original path"""
        ...
