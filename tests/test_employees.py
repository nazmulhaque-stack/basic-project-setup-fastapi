"""Unit and integration tests for Employee CRUD, 1-to-1 User relationship, and validation."""

from datetime import datetime, timezone
import uuid
import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.core.exceptions import ConflictError, NotFoundError
from app.db.base import Base
from app.models.employee import Employee
from app.models.user import User
from app.repositories.employee import EmployeeRepository
from app.schemas.employee import EmployeeCreate, EmployeeRead, EmployeeUpdate
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


@pytest.mark.asyncio
async def test_employee_service_crud_and_one_to_one_constraint() -> None:
    """Test full EmployeeService CRUD and strict 1-to-1 User constraint."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        # 1. Create a user first
        user1_id = uuid.uuid4()
        user1 = User(
            id=user1_id,
            name="John Doe",
            email="john.doe@example.com",
            role="user",
            phone_encrypted="encrypted_str",
            phone_blind_index="blind_index_1",
            created_at=datetime.now(timezone.utc),
        )
        session.add(user1)

        user2_id = uuid.uuid4()
        user2 = User(
            id=user2_id,
            name="Jane Smith",
            email="jane.smith@example.com",
            role="user",
            phone_encrypted="encrypted_str",
            phone_blind_index="blind_index_2",
            created_at=datetime.now(timezone.utc),
        )
        session.add(user2)
        await session.commit()

        service = EmployeeService(db=session)

        # 2. Test create employee with non-existent user -> raises NotFoundError
        non_existent_uid = uuid.uuid4()
        with pytest.raises(NotFoundError):
            await service.create(
                EmployeeCreate(
                    name="Ghost Employee",
                    age=28,
                    location="Chicago",
                    user_id=non_existent_uid,
                )
            )

        # 3. Create employee successfully for user1
        created = await service.create(
            EmployeeCreate(
                name="John Employee",
                age=32,
                location="San Francisco",
                user_id=user1_id,
            )
        )
        assert created.id is not None
        assert created.name == "John Employee"
        assert created.age == 32
        assert created.location == "San Francisco"
        assert created.user_id == user1_id

        # 4. Enforce 1-to-1: Trying to create another employee for user1 -> raises ConflictError
        with pytest.raises(ConflictError):
            await service.create(
                EmployeeCreate(
                    name="Duplicate Employee",
                    age=40,
                    location="Boston",
                    user_id=user1_id,
                )
            )

        # 5. Read by ID
        fetched = await service.get_by_id(created.id)
        assert fetched.id == created.id
        assert fetched.name == "John Employee"

        # 6. Read by user_id
        by_user = await service.get_by_user_id(user1_id)
        assert by_user is not None
        assert by_user.id == created.id

        # 7. Update employee
        updated = await service.update_by_id(
            created.id,
            EmployeeUpdate(location="Seattle", age=33),
        )
        assert updated.location == "Seattle"
        assert updated.age == 33
        assert updated.name == "John Employee"

        # 8. List employees
        items = await service.get_all()
        assert len(items) == 1
        assert items[0].id == created.id

        # 9. Delete employee
        await service.delete_by_id(created.id)

        # Verify deletion
        assert await service.get(created.id) is None
        assert await service.get_by_user_id(user1_id) is None

    await engine.dispose()


@pytest.mark.asyncio
async def test_employee_relationship_access() -> None:
    """Test ORM bidirectional relationship access between User and Employee."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async with session_factory() as session:
        user_id = uuid.uuid4()
        user = User(
            id=user_id,
            name="Rel User",
            email="rel.user@example.com",
            role="user",
            phone_encrypted="enc",
            phone_blind_index="blind_rel",
            created_at=datetime.now(timezone.utc),
        )
        employee_id = uuid.uuid4()
        employee = Employee(
            id=employee_id,
            name="Rel Employee",
            age=29,
            location="Austin",
            user_id=user_id,
            created_at=datetime.now(timezone.utc),
        )
        session.add_all([user, employee])
        await session.commit()

        # Query user and inspect employee via async selectinload
        res_user = await session.execute(
            select(User).options(selectinload(User.employee)).where(User.id == user_id)
        )
        loaded_user = res_user.scalar_one()
        assert loaded_user is not None
        assert loaded_user.employee is not None
        assert loaded_user.employee.id == employee_id
        assert loaded_user.employee.location == "Austin"

        # Query employee and inspect user via async selectinload
        res_emp = await session.execute(
            select(Employee).options(selectinload(Employee.user)).where(Employee.id == employee_id)
        )
        loaded_employee = res_emp.scalar_one()
        assert loaded_employee is not None
        assert loaded_employee.user is not None
        assert loaded_employee.user.id == user_id
        assert loaded_employee.user.name == "Rel User"

    await engine.dispose()


@pytest.mark.asyncio
async def test_employee_api_crud_endpoints() -> None:
    """Integration test for Employee CRUDRouter HTTP endpoints."""
    from httpx import ASGITransport, AsyncClient
    from app.main import app
    from app.db.session import get_session

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    user_id = uuid.uuid4()
    async with session_factory() as session:
        user = User(
            id=user_id,
            name="Endpoint User",
            email="endpoint.user@example.com",
            role="user",
            phone_encrypted="enc",
            phone_blind_index="blind_endpoint",
            created_at=datetime.now(timezone.utc),
        )
        session.add(user)
        await session.commit()

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. POST /api/v1/employees/
        create_payload = {
            "name": "Alice Developer",
            "age": 27,
            "location": "Seattle",
            "user_id": str(user_id),
        }
        resp = await client.post("/api/v1/employees", json=create_payload)
        assert resp.status_code == 201, resp.text
        data = resp.json()
        emp_id = data["id"]
        assert data["name"] == "Alice Developer"
        assert data["age"] == 27
        assert data["location"] == "Seattle"
        assert data["user_id"] == str(user_id)

        # 2. GET /api/v1/employees/{id}
        resp_get = await client.get(f"/api/v1/employees/{emp_id}")
        assert resp_get.status_code == 200
        assert resp_get.json()["id"] == emp_id

        # 3. GET /api/v1/employees/ (list with filters)
        resp_list = await client.get("/api/v1/employees?location=Seattle")
        assert resp_list.status_code == 200
        list_data = resp_list.json()
        assert len(list_data["items"]) == 1
        assert list_data["items"][0]["id"] == emp_id

        # 4. PATCH /api/v1/employees/{id}
        resp_patch = await client.patch(
            f"/api/v1/employees/{emp_id}",
            json={"location": "Austin", "age": 28},
        )
        assert resp_patch.status_code == 200
        assert resp_patch.json()["location"] == "Austin"
        assert resp_patch.json()["age"] == 28

        # 5. DELETE /api/v1/employees/{id}
        resp_del = await client.delete(f"/api/v1/employees/{emp_id}")
        assert resp_del.status_code in (200, 204)

        # 6. Verify 404 after delete
        resp_404 = await client.get(f"/api/v1/employees/{emp_id}")
        assert resp_404.status_code == 404

    app.dependency_overrides.clear()
    await engine.dispose()
