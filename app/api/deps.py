"""FastAPI dependencies: session -> service. Endpoints only ever see services."""

from collections.abc import Callable
from typing import Annotated, TypeVar

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_session
from app.services import BaseService, CategoryService, ProductService

ServiceT = TypeVar("ServiceT", bound=BaseService)  # type: ignore[type-arg]

SessionDep = Annotated[AsyncSession, Depends(get_session)]


def provide(service_cls: type[ServiceT]) -> Callable[[AsyncSession], ServiceT]:
    """Build a dependency for any service class: provide(ProductService)."""

    def _provider(session: SessionDep) -> ServiceT:
        return service_cls(session)

    _provider.__name__ = f"get_{service_cls.__name__}"
    return _provider


get_category_service = provide(CategoryService)
get_product_service = provide(ProductService)

CategoryServiceDep = Annotated[CategoryService, Depends(get_category_service)]
ProductServiceDep = Annotated[ProductService, Depends(get_product_service)]
