# FastAPI · OOP Repository Pattern · Dynamic CRUD · Pydantic v2

A production-style template: **Router → Service → Repository → Database**, where every
layer is a generic base class, and a new resource gets full CRUD (pagination, typed
filters, sorting, validation, proper error codes) with ~15 lines of code.

Tested with Python 3.13, FastAPI 0.162, SQLAlchemy 2.1 (async), Pydantic 2.14,
on SQLite and MariaDB 10.11 (MySQL-compatible). **13/13 tests pass on both.**

```mermaid
flowchart LR
    C[Client] -->|HTTP JSON| R[CRUDRouter<br/>api/]
    R -->|Pydantic schema| S[BaseService<br/>services/]
    S -->|plain dict| P[SQLAlchemyRepository<br/>repositories/]
    P -->|SQL| D[(MySQL)]
    S -. commit .-> D
    P -. raises NotFound / Conflict .-> S
    S -. domain errors .-> H[Exception handler<br/>→ 404 / 409 / 422]
```

---

## 1. Run it

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # edit DATABASE_URL for your MySQL
fastapi dev app/main.py         # open http://127.0.0.1:8000/docs
pytest                          # SQLite, no setup needed
TEST_DATABASE_URL="mysql+aiomysql://user:pass@localhost:3306/app_test" pytest   # real MySQL
```

> `TEST_DATABASE_URL` must point to an **empty throwaway** database: tests drop all tables.

---

## 2. Folder structure

```
app/
├── core/
│   ├── config.py          # Settings (pydantic-settings, reads .env)
│   ├── database.py        # async engine, session factory, get_session dependency
│   └── exceptions.py      # AppError → NotFound(404) / Conflict(409) / BusinessRule(422)
├── models/                # SQLAlchemy 2 typed models
│   ├── base.py            # Base + IdMixin + TimestampMixin + constraint naming
│   ├── category.py
│   └── product.py
├── schemas/               # Pydantic v2
│   ├── base.py            # BaseSchema, InputSchema, ReadSchema, Page[T], make_partial()
│   ├── category.py
│   └── product.py
├── repositories/          # data access only
│   ├── base.py            # AbstractRepository (ABC) + generic SQLAlchemyRepository[Model]
│   ├── category.py        # 1 line: inherits everything
│   └── product.py         # + custom queries (eager load, atomic stock update)
├── services/              # business rules + transaction boundary
│   ├── base.py            # BaseService[Model, Create, Update] with hooks
│   ├── category.py        # 1 line
│   └── product.py         # rules via hooks + custom use cases
├── api/
│   ├── crud_router.py     # CRUDRouter: the dynamic CRUD generator
│   ├── deps.py            # session → service dependencies
│   └── v1/
│       ├── categories.py  # 100% generated
│       └── products.py    # generated + 3 custom endpoints
└── main.py                # app factory, lifespan, exception handlers
tests/                     # API tests + unit tests (mocked repository)
```

---

## 3. Add a new resource in 4 small files

Example: `Brand`.

**models/brand.py**
```python
class Brand(IdMixin, TimestampMixin, Base):
    __tablename__ = "brands"
    name: Mapped[str] = mapped_column(String(100), unique=True)
    country: Mapped[str | None] = mapped_column(String(60))
```
(+ add it to `models/__init__.py`)

**schemas/brand.py**
```python
class BrandCreate(InputSchema):
    name: str = Field(min_length=2, max_length=100)
    country: str | None = None

BrandUpdate = make_partial(BrandCreate, name="BrandUpdate")   # PATCH schema, generated

class BrandRead(ReadSchema):
    name: str
    country: str | None
```

**repositories/brand.py + services/brand.py**
```python
class BrandRepository(SQLAlchemyRepository[Brand]):   # model inferred from [Brand]
    pass

class BrandService(BaseService[Brand, BrandCreate, BrandUpdate]):
    repository_class = BrandRepository
```

**api/v1/brands.py**
```python
from app.api.crud_router import CRUDRouter
from app.api.deps import provide

router = CRUDRouter(
    model=Brand,
    service=provide(BrandService),
    create_schema=BrandCreate,
    update_schema=BrandUpdate,
    read_schema=BrandRead,
    prefix="/brands",
    filter_fields={"name": ["icontains"], "country": ["eq", "in"]},
    sort_fields=["name", "created_at"],
)
```
Include it in `api/v1/__init__.py`. Done: 6 endpoints, documented in `/docs`.

---

## 4. What the generated endpoints do

| Method | Path | Body | Success |
|---|---|---|---|
| GET | `/products` | – | `Page[ProductRead]` |
| GET | `/products/{id}` | – | `ProductRead` |
| POST | `/products` | `ProductCreate` | 201 `ProductRead` |
| PATCH | `/products/{id}` | `ProductUpdate` (only sent fields change) | `ProductRead` |
| PUT | `/products/{id}` | `ProductCreate` (full replace) | `ProductRead` |
| DELETE | `/products/{id}` | – | 204 |

Turn routes off with `routes=("list", "get")`, or protect some:
`route_dependencies={"delete": [Depends(require_admin)]}`.

### Filtering, sorting, paging

```
GET /api/v1/products?price__gte=20000&price__lte=60000&name__icontains=galaxy
GET /api/v1/products?category_id__in=1,2,3          (or repeat: ?category_id__in=1&category_id__in=2)
GET /api/v1/products?sort=-price,name&page=2&size=10
```

Operators: `eq` (plain name), `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`,
`contains`, `icontains`, `startswith`, `isnull`.
Add one in `FILTER_OPERATORS` (repositories/base.py) and every resource gets it.

The router turns `filter_fields` into a real Pydantic model (`ProductListQuery`) at
startup, typed from the column types. So:
* `/docs` shows each filter with the right type (int, decimal, bool…)
* `?stock__lte=many` → 422, `?prise__gte=1` (typo) → 422, `?sort=secret_col` → 422
* bad config (`filter_fields={"nope": ["eq"]}`) fails at **startup**, not in production traffic

Response:
```json
{ "items": [...], "total": 42, "page": 2, "size": 10, "pages": 5 }
```

---

## 5. Why it is built this way (the "deep" part)

**Each layer has one job (SRP)**
* Repository = how to read/write rows. No HTTP, no business rules, never commits.
* Service = business rules + the transaction. Converts schemas → dicts, calls `commit()`.
* Router = HTTP only: parse, validate, call service, shape the response.

**Services depend on an interface (DIP).** `AbstractRepository` is the contract.
`tests/test_units.py` swaps the repository for a mock and tests a business rule with no
database at all.

**Template Method for business rules (OCP).** `BaseService.create/update/delete` are
written once; subclasses override `before_create`, `before_update`, `before_delete`.
`ProductService` blocks products in a missing or inactive category this way, without
rewriting create/update.

**Commit in the service, not in the session dependency.** Since FastAPI 0.118, code
after `yield` in a dependency (default scope) runs *after* the response is sent. If you
commit there and it fails, the client already got 201 for data that was never saved.
Committing in the service keeps failures inside the request.

**`expire_on_commit=False`.** With `AsyncSession`, expired attributes trigger hidden
I/O on access → `MissingGreenlet`. This flag plus `refresh()` after flush (to load
server defaults like `created_at`) avoids it.

**Relationships are `lazy="raise"`.** Accidental lazy loading is the #1 async
SQLAlchemy bug. With `raise`, it fails loudly in development; you load relations
explicitly (`ProductRepository.get_with_category` uses `selectinload`).

**Constraint errors become 409, raw DB errors stay in logs.** Uniqueness is enforced by
the database (race-safe), and `IntegrityError` is mapped to `ConflictError`. The client
never sees table/column names from the DB message.

**`make_partial()` for PATCH.** The update schema is generated from the create schema:
every field becomes optional but keeps its type, constraints and validators. Sending
`{"name": null}` for a non-nullable field gives 422 instead of a database error, and
`model_dump(exclude_unset=True)` changes only what the client sent.

**Input schemas forbid unknown fields.** `{"name": "x", "is_admin": true}` → 422, so
mass-assignment of fields you did not plan for is impossible. The repository also
ignores `id`, `created_at`, `updated_at` even if a schema includes them.

**Atomic updates where races matter.** `POST /products/{id}/stock` runs one
`UPDATE … SET stock = stock + :d WHERE stock + :d >= 0`, so two buyers can never
oversell. Read-modify-write in Python would.

**Escaped LIKE.** `contains`/`icontains` use `autoescape=True`: a user searching `%`
finds a literal `%`, not every row.

**Stable pagination.** Every list query ends with `ORDER BY …, id`, so rows never
repeat or disappear between pages.

**MySQL specifics handled.** `pool_pre_ping` + `pool_recycle` (MySQL drops idle
connections), connections set to UTC, timestamps returned as `…Z`, money as
`DECIMAL(12,2)` (JSON string `"38999.00"`, never float), deterministic constraint
names for Alembic.

**Custom routes never clash with generated ones.** Generated item routes use
`/{item_id:int}`, so `/products/low-stock` can be added after them.

---

## 6. Production checklist

* Replace `AUTO_CREATE_TABLES` with **Alembic**:
  `alembic init -t async migrations`, then in `env.py` set
  `target_metadata = app.models.Base.metadata`.
* Add auth (your JWT dependency) through `route_dependencies` or router `dependencies`.
* Optional extensions that fit this design: soft delete (`deleted_at` mixin + default
  filter in the repository), optimistic locking (`version_id_col`), cursor pagination
  for very large tables.

## Sources
* FastAPI – dependencies with yield, scopes and version history: https://fastapi.tiangolo.com/advanced/advanced-dependencies
* FastAPI – query parameter models: https://fastapi.tiangolo.com/tutorial/query-param-models/
* FastAPI – partial updates with `exclude_unset`: https://fastapi.tiangolo.com/tutorial/body-updates/
* Pydantic v2 changelog: https://pydantic.dev/docs/validation/latest/get-started/changelog/
