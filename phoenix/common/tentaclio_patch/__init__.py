"""Tentaclio Patch.

This patch implements the S3Client.
This is a temporary patch while waiting for a PR to be merged
on the tentaclio repo.
"""
from tentaclio.fs import SCANNER_REGISTRY
from tentaclio.fs.copier import COPIER_REGISTRY
from tentaclio.fs.remover import REMOVER_REGISTRY, ClientRemover
from tentaclio.fs.scanners import ClientDirScanner
from tentaclio.streams import stream_client_handler, stream_registry

from phoenix.common.tentaclio_patch import s3_client


# We have to pop the old client for each registry or the new one will not be added
# stream
stream_registry.STREAM_HANDLER_REGISTRY.registry.pop("s3")
stream_registry.STREAM_HANDLER_REGISTRY.register(
    "s3", stream_client_handler.StreamURLHandler(s3_client.S3Client)
)

# scanner
SCANNER_REGISTRY.registry.pop("s3")
SCANNER_REGISTRY.register("s3", ClientDirScanner(s3_client.S3Client))

# copy
COPIER_REGISTRY.registry.pop("s3+s3")
COPIER_REGISTRY.register("s3+s3", s3_client.S3Client("s3://"))

# remove
REMOVER_REGISTRY.registry.pop("s3")
REMOVER_REGISTRY.register("s3", ClientRemover(s3_client.S3Client))
