"""Unit tests: repository directly, service with a mocked repository, schema factory."""

from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from app.core.exceptions import BusinessRuleError, InvalidQueryError
from app.repositories import CategoryRepository, ProductRepository
from app.schemas import ProductCreate, ProductUpdate
from app.services import ProductService


async def test_repository_generic_crud(session):
    repo = CategoryRepository(session)
    assert repo.model.__name__ == "Category"  # inferred from CategoryRepository[Category]

    created = await repo.create_many([{"name": "B"}, {"name": "A"}, {"name": "C", "is_active": False}])
    await session.commit()
    assert all(c.id for c in created)

    assert [c.name for c in await repo.list()] == ["A", "B", "C"]  # default_sort = name
    assert await repo.count(filters={"is_active": True}) == 2
    assert await repo.exists(name="C")
    assert (await repo.get_by(name="A")).name == "A"

    # protected fields are ignored, unknown fields are rejected
    obj = await repo.update(created[0], {"id": 999, "name": "B2"})
    assert obj.id != 999 and obj.name == "B2"
    with pytest.raises(InvalidQueryError):
        await repo.update(obj, {"not_a_column": 1})
    with pytest.raises(InvalidQueryError):
        await repo.list(filters={"name__regex": "x"})
    with pytest.raises(InvalidQueryError):
        await repo.delete_where()  # never delete a whole table by accident

    assert await repo.delete_where(is_active=False) == 1


async def test_service_rule_with_mocked_repository(session):
    """The service depends on the repository *interface*, so it can be faked."""
    service = ProductService(session)
    service.categories = AsyncMock(spec=CategoryRepository)
    service.categories.exists.return_value = False
    service.repository = AsyncMock(spec=ProductRepository)

    payload = ProductCreate(name="Phone", sku="abc", price="10", category_id=1)
    with pytest.raises(BusinessRuleError):
        await service.create(payload)
    service.repository.create.assert_not_awaited()  # rule blocked the write


def test_partial_schema_generated_from_create_schema():
    assert ProductUpdate.model_validate({}).model_dump(exclude_unset=True) == {}
    assert ProductUpdate.model_validate({"sku": "x-1"}).sku == "X-1"  # validator kept
    for bad in ({"price": -1}, {"name": None}, {"unknown": 1}):
        with pytest.raises(ValidationError):
            ProductUpdate.model_validate(bad)
