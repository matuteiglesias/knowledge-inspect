# pipeline/embedding/adapters.py
from typing import Optional, Iterable
import hashlib

from shared.config import EMBED_DIM, EMBED_ADAPTER


# from typing import List

# class EmbeddingAdapterProtocol:
#     @property
#     def dim(self) -> int: raise NotImplementedError
#     def embed_batch(self, ids: List[str], texts: List[str]) -> List[List[float]]: raise NotImplementedError
#     def embed_one(self, id: str, text: str) -> List[float]: raise NotImplementedError
#     def close(self) -> None: return



class EmbeddingAdapter:
    def __call__(self, text_id: str, text: str) -> Iterable[float]:
        raise NotImplementedError()
    @property
    def dim(self) -> Optional[int]:
        return None

# # --- Adapter protocol --- (duck-typed protocol for clarity / typing)
# class EmbeddingAdapterProtocol(Protocol):
#     dim: Optional[int]  # optional known output dimension
#     def embed_batch(self, ids: List[str], texts: List[str]) -> List[List[float]]: ...
#     def embed_one(self, id: str, text: str) -> List[float]: ...
#     def set_seed(self, seed: int) -> None: ...  # optional

# # --- Minimal cache expected interface (duck-typed) ---
# # Preferred methods:
# #   cache.get_many(ids: List[str]) -> Dict[str, List[float]]
# #   cache.set_many(mapping: Dict[str, List[float]]) -> None
# # Fallback methods:
# #   cache.get(id) -> Optional[List[float]]
# #   cache.set(id, vector) -> None



# adapter should implement embed_batch(ids, texts) and embed_one(id, text) if possible. Legacy names (batch, batch_encode) are tolerated for embed_batch. If adapter is callable, it will be used per-item as fallback.

class PlaceholderAdapter(EmbeddingAdapter):
    def __init__(self, dim: int = 128):
        self._dim = int(dim)
    def __call__(self, text_id: str, text: str):
        h = hashlib.sha1((text or "").encode("utf8")).hexdigest()
        out = []
        for i in range(self._dim):
            idx = (i * 4) % len(h)
            out.append(int(h[idx:idx+4], 16) % 1000 / 1000.0)
        return out
    @property
    def dim(self) -> Optional[int]:
        return self._dim



class LlamaIndexAdapter(EmbeddingAdapter):
    def __init__(self, provider, dim: Optional[int] = None):
        self.provider = provider
        self._dim = dim
    def __call__(self, text_id: str, text: str):
        if hasattr(self.provider, "get_text_embedding"):
            vec = self.provider.get_text_embedding(text)
        elif callable(self.provider):
            vec = self.provider(text)
        elif hasattr(self.provider, "embed"):
            vec = self.provider.embed(text)
        else:
            raise RuntimeError("Provider has no recognized embed API")
        if hasattr(vec, "tolist"):
            return vec.tolist()
        try:
            return list(vec)
        except Exception:
            # fallback deterministic
            return PlaceholderAdapter(dim=self._dim or 128)(text_id, text)
    @property
    def dim(self) -> Optional[int]:
        return self._dim


# local wrapper to create a default adapter
def _build_default_adapter(name: str = None, dim: int = None, provider: Optional[object] = None):
    name = name or EMBED_ADAPTER
    dim = int(dim or EMBED_DIM)
    if name == "placeholder":
        if PlaceholderAdapter is None:
            # minimal local placeholder
            class _P:
                def __init__(self, d): self._d = int(d)
                def __call__(self, tid, txt):
                    h = hashlib.sha1((txt or "").encode("utf8")).hexdigest()
                    out = []
                    for i in range(self._d):
                        idx = (i * 4) % len(h)
                        out.append(int(h[idx:idx+4], 16) % 1000 / 1000.0)
                    return out
            return _P(dim)
        return PlaceholderAdapter(dim=dim)
    if name == "llamaindex":
        if provider is None:
            raise RuntimeError("llamaindex adapter requested but no provider supplied")
        return LlamaIndexAdapter(provider, dim=dim)
    raise RuntimeError(f"Unknown adapter name: {name}")
