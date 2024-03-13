import urllib3.util

import fsspec
import s3fs
import yaml  # type: ignore

from monde.schema import spec


def read(filepath: str, **storage_options) -> spec.SchemaModel:
    def get_filesystem(filepath: str, **storage_options) -> fsspec.spec.AbstractFileSystem:
        uri = urllib3.util.parse_url(filepath)
        return (
            s3fs.S3FileSystem(**storage_options)
            if uri.scheme == "s3" else
            fsspec.filesystem(uri.scheme, **storage_options)
        )

    # Build a filesystem either using s3fs or fsspec
    fs = get_filesystem(filepath, **storage_options)

    # Read the file into a schema model
    with fs.open(filepath) as fh:
        return spec.Schema(**yaml.safe_load(fh))
