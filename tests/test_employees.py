"""Unit and integration tests for Employee CRUD, 1-to-1 User relationship, and validation.

Integration tests here run against a real Postgres connection via the `pg_pool`
fixture (see `tests/conftest.py`) and self-clean their rows, since there is no
in-memory substitute that faithfully reproduces asyncpg/Postgres behavior now
that the project talks to the database only via raw asyncpg (no SQLAlchemy, no
SQLite fallback). Bidirectional ORM relationship loading (`User.employee`,
`Employee.user`) was dropped along with SQLAlchemy — per the "hand-write SQL
per repository" decision, nothing in the current repositories populates those
optional dataclass fields, so there's no such behavior left to test here.
"""

from typing import Any
import uuid
import pytest
from pydantic import ValidationError

from app.core.exceptions import ConflictError, NotFoundError
from app.db.pool import UnitOfWork
from app.schemas.employee import EmployeeCreate, EmployeeUpdate
from app.services.employee import EmployeeService


def test_employee_schema_validation() -> None:
    """Test Pydantic validation rules on Employee schemas."""
    user_id = uuid.uuid4()

    # Valid creation payload
    emp = EmployeeCreate(name="Alice Doe", age=30, location="New York", user_id=user_id)
    assert emp.name == "Alice Doe"
    assert emp.age == 30
    assert emp.location == "New York"
    assert emp.user_id == user_id

    # Invalid age: under 18
    with pytest.raises(ValidationError):
        EmployeeCreate(name="Young", age=16, location="New York", user_id=user_id)

    # Invalid age: over 120
    with pytest.raises(ValidationError):
        EmployeeCreate(name="Old", age=150, location="New York", user_id=user_id)

    # Invalid name: empty
    with pytest.raises(ValidationError):
        EmployeeCreate(name="", age=25, location="New York", user_id=user_id)


async def add_user(pool: Any, *, name: str, email: str) -> uuid.UUID:
    user_id = uuid.uuid4()
    async with pool.acquire() as connection:
        await connection.execute(
            """
            INSERT INTO users (id, name, email, role, phone_encrypted, phone_blind_index)
            VALUES ($1, $2, $3, $4, $5, $6)
            """,
            user_id,
            name,
            email,
            "user",
            "encrypted_str",
            f"blind_{user_id}",
        )
    return user_id


async def cleanup_users_and_employees(pool: Any, user_ids: list[uuid.UUID]) -> None:
    async with pool.acquire() as connection:
        await connection.execute(
            "DELETE FROM employees WHERE user_id = ANY($1::uuid[])", user_ids
        )
        await connection.execute("DELETE FROM users WHERE id = ANY($1::uuid[])", user_ids)


@pytest.mark.asyncio
async def test_employee_service_crud_and_one_to_one_constraint(pg_pool: Any) -> None:
    """Test full EmployeeService CRUD and strict 1-to-1 User constraint."""
    unique = uuid.uuid4().hex[:8]
    user1_id = await add_user(pg_pool, name="John Doe", email=f"john.doe.{unique}@example.com")
    user2_id = await add_user(pg_pool, name="Jane Smith", email=f"jane.smith.{unique}@example.com")

    try:
        async with pg_pool.acquire() as connection:
            async with UnitOfWork(connection) as uow:
                service = EmployeeService(db=uow)

                # 1. Create employee with non-existent user -> raises NotFoundError
                non_existent_uid = uuid.uuid4()
                with pytest.raises(NotFoundError):
                    await service.create(
                        EmployeeCreate(
                            name="Ghost Employee",
                            age=28,
                            location=f"Chicago-{unique}",
                            user_id=non_existent_uid,
                        )
                    )

                # 2. Create employee successfully for user1
                created = await service.create(
                    EmployeeCreate(
                        name="John Employee",
                        age=32,
                        location=f"San Francisco-{unique}",
                        user_id=user1_id,
                    )
                )
                assert created.id is not None
                assert created.name == "John Employee"
                assert created.age == 32
                assert created.location == f"San Francisco-{unique}"
                assert created.user_id == user1_id

                # 3. Enforce 1-to-1: another employee for user1 -> raises ConflictError
                with pytest.raises(ConflictError):
                    await service.create(
                        EmployeeCreate(
                            name="Duplicate Employee",
                            age=40,
                            location=f"Boston-{unique}",
                            user_id=user1_id,
                        )
                    )

                # 4. Read by ID
                fetched = await service.get_by_id(created.id)
                assert fetched.id == created.id
                assert fetched.name == "John Employee"

                # 5. Read by user_id
                by_user = await service.get_by_user_id(user1_id)
                assert by_user is not None
                assert by_user.id == created.id

                # 6. Update employee
                updated = await service.update_by_id(
                    created.id,
                    EmployeeUpdate(location=f"Seattle-{unique}", age=33),
                )
                assert updated.location == f"Seattle-{unique}"
                assert updated.age == 33
                assert updated.name == "John Employee"

                # 7. List employees scoped to this test's unique location
                items = await service.get_all(location=f"Seattle-{unique}")
                assert len(items) == 1
                assert items[0].id == created.id

                # 8. Delete employee
                await service.delete_by_id(created.id)

                # Verify deletion
                assert await service.get(created.id) is None
                assert await service.get_by_user_id(user1_id) is None
    finally:
        await cleanup_users_and_employees(pg_pool, [user1_id, user2_id])


@pytest.mark.asyncio
async def test_employee_api_crud_endpoints(pg_pool: Any) -> None:
    """Integration test for Employee CRUDRouter HTTP endpoints."""
    from httpx import ASGITransport, AsyncClient
    from app.main import app
    from app.db.pool import get_session

    unique = uuid.uuid4().hex[:8]
    user_id = await add_user(pg_pool, name="Endpoint User", email=f"endpoint.user.{unique}@example.com")

    async def override_get_session():
        async with pg_pool.acquire() as connection:
            async with UnitOfWork(connection) as uow:
                yield uow

    app.dependency_overrides[get_session] = override_get_session

    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            # 1. POST /api/v1/employees/
            create_payload = {
                "name": "Alice Developer",
                "age": 27,
                "location": f"Seattle-{unique}",
                "user_id": str(user_id),
            }
            resp = await client.post("/api/v1/employees", json=create_payload)
            assert resp.status_code == 201, resp.text
            data = resp.json()
            emp_id = data["id"]
            assert data["name"] == "Alice Developer"
            assert data["age"] == 27
            assert data["location"] == f"Seattle-{unique}"
            assert data["user_id"] == str(user_id)

            # 2. GET /api/v1/employees/{id}
            resp_get = await client.get(f"/api/v1/employees/{emp_id}")
            assert resp_get.status_code == 200
            assert resp_get.json()["id"] == emp_id

            # 3. GET /api/v1/employees/ (list with filters)
            resp_list = await client.get(f"/api/v1/employees?location={data['location']}")
            assert resp_list.status_code == 200
            list_data = resp_list.json()
            assert len(list_data["items"]) == 1
            assert list_data["items"][0]["id"] == emp_id

            # 4. PATCH /api/v1/employees/{id}
            resp_patch = await client.patch(
                f"/api/v1/employees/{emp_id}",
                json={"location": f"Austin-{unique}", "age": 28},
            )
            assert resp_patch.status_code == 200
            assert resp_patch.json()["location"] == f"Austin-{unique}"
            assert resp_patch.json()["age"] == 28

            # 5. DELETE /api/v1/employees/{id}
            resp_del = await client.delete(f"/api/v1/employees/{emp_id}")
            assert resp_del.status_code in (200, 204)

            # 6. Verify 404 after delete
            resp_404 = await client.get(f"/api/v1/employees/{emp_id}")
            assert resp_404.status_code == 404
    finally:
        app.dependency_overrides.clear()
        await cleanup_users_and_employees(pg_pool, [user_id])
