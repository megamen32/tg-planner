from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ProductRaw:
    """Raw representation of a Wildberries product (минимальный набор полей)."""

    id: int | None
    name: str | None
    brand: str | None
    supplier: str | None
    description: str | None
    sources: dict[str, dict[str, Any]]

    price: int | None = None
    sale_price: int | None = None
    rating: float | None = None
    feedbacks: int | None = None

    category_id: int | None = None
    category_parent_id: int | None = None
    root: int | None = None
    kind_id: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "brand": self.brand,
            "supplier": self.supplier,
            "description": self.description,
            "sources": self.sources,
            "price": self.price,
            "sale_price": self.sale_price,
            "rating": self.rating,
            "feedbacks": self.feedbacks,
            "category_id": self.category_id,
            "category_parent_id": self.category_parent_id,
            "root": self.root,
            "kind_id": self.kind_id,
        }


@dataclass(slots=True)
class ProductNormalized:
    """Normalized representation: чистый текст, унифицированные поля."""

    id: int
    title: str
    description: str
    brand: str | None
    category: str | None
    price: float | None
    rating: float | None
    feedbacks: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "brand": self.brand,
            "category": self.category,
            "price": self.price,
            "rating": self.rating,
            "feedbacks": self.feedbacks,
        }


@dataclass(slots=True)
class ProductIndexDoc:
    """Документ для индексации: объединённый текст и метаданные."""

    id: int
    text_index: str
    brand: str | None
    category: str | None
    price: float | None
    rating: float | None
    feedbacks: int | None

    embedding: list[float] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "text_index": self.text_index,
            "brand": self.brand,
            "category": self.category,
            "price": self.price,
            "rating": self.rating,
            "feedbacks": self.feedbacks,
            "embedding": self.embedding,
        }
