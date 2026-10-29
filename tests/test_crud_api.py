"""End-to-end tests through HTTP: router -> service -> repository -> DB."""

from decimal import Decimal


# ------------------------------------------------------------------ generic CRUD
async def test_category_full_lifecycle(client):
    r = await client.post("/categories", json={"name": "  Tablets ", "description": "big"})
    assert r.status_code == 201
    cat = r.json()
    assert cat["name"] == "Tablets"  # str_strip_whitespace
    assert cat["id"] > 0 and cat["created_at"] and cat["updated_at"]

    assert (await client.get(f"/categories/{cat['id']}")).json()["name"] == "Tablets"

    r = await client.patch(f"/categories/{cat['id']}", json={"description": None})
    assert r.status_code == 200
    assert r.json()["description"] is None and r.json()["name"] == "Tablets"

    r = await client.put(f"/categories/{cat['id']}", json={"name": "Tabs"})
    assert r.status_code == 200
    assert r.json() | {"updated_at": None} == {
        **cat, "name": "Tabs", "description": None, "is_active": True, "updated_at": None
    }

    assert (await client.delete(f"/categories/{cat['id']}")).status_code == 204
    r = await client.get(f"/categories/{cat['id']}")
    assert r.status_code == 404
    assert r.json() == {"detail": f"Category with id={cat['id']} not found.", "code": "not_found"}


async def test_unique_constraint_maps_to_409(client, category):
    r = await client.post("/categories", json={"name": "Smartphones"})
    assert r.status_code == 409
    assert r.json()["code"] == "conflict"
    assert "details" not in r.json()  # raw DB errors are never leaked


async def test_input_validation(client, category):
    cid = category["id"]
    # unknown field rejected (extra="forbid")
    assert (await client.post("/categories", json={"name": "Ok", "hack": 1})).status_code == 422
    # PATCH: explicit null on a non-nullable field -> 422 instead of a DB error
    assert (await client.patch(f"/categories/{cid}", json={"name": None})).status_code == 422
    # PATCH: constraints of the create schema still apply
    assert (await client.patch(f"/categories/{cid}", json={"name": "x"})).status_code == 422
    # PATCH: empty body is a no-op
    r = await client.patch(f"/categories/{cid}", json={})
    assert r.status_code == 200 and r.json() == category
    # non-int id never reaches the handler
    assert (await client.get("/categories/abc")).status_code == 404


# ------------------------------------------------------------ business rules
async def test_product_requires_active_category(client, category):
    body = {"name": "Phone", "sku": "abc", "price": "10", "category_id": 999}
    r = await client.post("/products", json=body)
    assert r.status_code == 422 and r.json()["code"] == "business_rule_violation"

    await client.patch(f"/categories/{category['id']}", json={"is_active": False})
    r = await client.post("/products", json={**body, "category_id": category["id"]})
    assert r.status_code == 422


async def test_sku_validator_and_decimal(client, products):
    p = products[0]
    assert p["sku"] == "SAM-A55"  # field_validator upper-cases
    assert Decimal(p["price"]) == Decimal("38999.00")


async def test_cannot_delete_category_in_use(client, category, products):
    r = await client.delete(f"/categories/{category['id']}")
    assert r.status_code == 409


# ----------------------------------------------- dynamic filters / sort / paging
async def test_filters_sort_pagination(client, products, category):
    r = await client.get("/products", params={"price__gte": "40000", "sort": "-price"})
    data = r.json()
    assert r.status_code == 200
    assert [i["name"] for i in data["items"]] == ["iPhone 15", "Galaxy S24", "Pixel 8a"]
    assert data["total"] == 3

    r = await client.get("/products", params={"name__icontains": "galaxy", "sort": "name"})
    assert [i["name"] for i in r.json()["items"]] == ["Galaxy A55", "Galaxy S24"]

    r = await client.get("/products", params={"size": 2, "page": 3, "sort": "id"})
    data = r.json()
    assert (data["total"], data["pages"], data["page"], len(data["items"])) == (5, 3, 3, 1)

    other = (await client.post("/categories", json={"name": "Feature phones"})).json()
    await client.post(
        "/products",
        json={"name": "Nokia 105", "sku": "nok-105", "price": "1999", "category_id": other["id"]},
    )
    a, b = category["id"], other["id"]
    csv = await client.get("/products", params={"category_id__in": f"{a},{b}"})  # 1,2
    rep = await client.get(f"/products?category_id__in={b}&category_id__in=999")  # repeated
    assert (csv.json()["total"], rep.json()["total"]) == (6, 1)

    r = await client.get("/products", params={"stock__lte": 2, "sort": "stock"})
    assert [i["stock"] for i in r.json()["items"]] == [0, 0, 2]  # incl. Nokia (default 0)


async def test_filter_safety(client, products):
    # typo / unknown query param -> 422, listing the offending param
    r = await client.get("/products", params={"prise__gte": 1})
    assert r.status_code == 422 and r.json()["detail"][0]["loc"] == ["query", "prise__gte"]
    # sorting by a column that is not whitelisted -> 422
    assert (await client.get("/products", params={"sort": "sku"})).status_code == 422
    # wrong type for a typed filter -> 422
    assert (await client.get("/products", params={"stock__lte": "many"})).status_code == 422
    # LIKE wildcards are escaped: "%" matches a literal percent sign, not everything
    r = await client.get("/products", params={"name__icontains": "%"})
    assert r.json()["total"] == 0
    # page size ceiling
    assert (await client.get("/products", params={"size": 1000})).status_code == 422


# ------------------------------------------------------------ custom endpoints
async def test_custom_routes_coexist_with_generated_ones(client, products):
    r = await client.get("/products/low-stock", params={"threshold": 2})
    assert [p["stock"] for p in r.json()] == [0, 2]

    pid = products[0]["id"]
    r = await client.get(f"/products/{pid}/detail")
    assert r.status_code == 200 and r.json()["category"]["name"] == "Smartphones"


async def test_atomic_stock_adjustment(client, products):
    pid = products[1]["id"]  # stock 3
    r = await client.post(f"/products/{pid}/stock", json={"delta": 5})
    assert r.status_code == 200 and r.json()["stock"] == 8

    r = await client.post(f"/products/{pid}/stock", json={"delta": -9})
    assert r.status_code == 422
    assert (await client.get(f"/products/{pid}")).json()["stock"] == 8

    assert (await client.post("/products/9999/stock", json={"delta": 1})).status_code == 404
