"""Fail-closed pipeline exceptions."""


class PipelineError(Exception):
    """Base error for rejected pipeline work."""


class SourceError(PipelineError):
    """A source is malformed, corrupt, substituted, or exceeds bounds."""


class SourceIdentityError(SourceError):
    """An authoritative source object changed identity during bounded acquisition."""


class IdentityError(PipelineError):
    """Official market identity or resolution evidence is invalid."""


class UnresolvedMarketError(IdentityError):
    """A fully bound official market has not published a final outcome."""

    def __init__(self, slug: str, market_id: str, condition_id: str):
        super().__init__("market is unresolved")
        self.slug = slug
        self.market_id = market_id
        self.condition_id = condition_id


class ReconstructionError(PipelineError):
    """A book stream cannot be reconstructed without ambiguity."""


class MissingInitialSnapshotError(ReconstructionError):
    """A bounded event stream contains no full order-book snapshot."""


class ConflictError(PipelineError):
    """An immutable identity already exists with different content."""


class ResourceLimitError(PipelineError):
    """A configured disk, memory, transfer, or asset bound was exceeded."""
