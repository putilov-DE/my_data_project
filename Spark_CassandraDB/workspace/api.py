import redis
import json
import uvicorn
from fastapi import FastAPI, HTTPException
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

# 1. Initialize FastAPI app
app = FastAPI(
    title="Amazon Reviews Analytics API",
    description="API with Redis Caching and Cassandra Backend"
)

# 2. Connect to Cassandra
print("Connecting to Cassandra...")
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('amazon')
session.row_factory = dict_factory
print("Cassandra connected successfully!")

# 3. Connect to Redis
print("Connecting to Redis...")
r = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
print("Redis connected successfully!")


# ==========================================
# ENDPOINTS
# ==========================================

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Amazon Reviews API with Redis cache is running"}


@app.get("/reviews/product/{product_id}")
def get_reviews_by_product(product_id: str, limit: int = 10):
    """Fetch raw reviews for a specific product with caching and date fix"""
    cache_key = f"product_reviews:{product_id}:{limit}"

    cached = r.get(cache_key)
    if cached:
        print(f"Cache hit for {cache_key}")
        return {"product_id": product_id, "reviews": json.loads(cached), "source": "cache"}

    print(f"Cache miss. Querying Cassandra for product: {product_id}")
    query = "SELECT * FROM reviews_by_product WHERE product_id = %s LIMIT %s"
    rows = session.execute(query, [product_id, limit])
    results = list(rows)

    if not results:
        raise HTTPException(status_code=404, detail="Product not found or no reviews exist")

    r.setex(cache_key, 600, json.dumps(results, default=str))

    return {"product_id": product_id, "reviews": results, "source": "database"}
@app.get("/top/products/{period}")
def get_top_products(period: str, limit: int = 10):
    """Fetch top reviewed products with caching"""
    cache_key = f"top_products:{period}:{limit}"

    cached = r.get(cache_key)
    if cached:
        print(f"Cache hit for {cache_key}")
        return {"period": period, "top_products": json.loads(cached), "source": "cache"}

    print(f"Cache miss. Querying Cassandra for period: {period}")
    query = "SELECT * FROM top_products_by_period WHERE period = %s LIMIT %s"
    rows = session.execute(query, [period, limit])
    results = list(rows)

    r.setex(cache_key, 600, json.dumps(results))
    return {"period": period, "top_products": results, "source": "database"}


@app.get("/top/haters/{period}")
def get_top_haters(period: str, limit: int = 10):
    """Fetch top haters with caching"""
    cache_key = f"top_haters:{period}:{limit}"

    cached = r.get(cache_key)
    if cached:
        print(f"Cache hit for {cache_key}")
        return {"period": period, "top_haters": json.loads(cached), "source": "cache"}

    print(f"Cache miss. Querying Cassandra for period: {period}")
    query = "SELECT * FROM top_haters WHERE period = %s LIMIT %s"
    rows = session.execute(query, [period, limit])
    results = list(rows)

    r.setex(cache_key, 600, json.dumps(results))
    return {"period": period, "top_haters": results, "source": "database"}


@app.get("/top/backers/{period}")
def get_top_backers(period: str, limit: int = 10):
    """Fetch top backers with caching"""
    cache_key = f"top_backers:{period}:{limit}"

    cached = r.get(cache_key)
    if cached:
        print(f"Cache hit for {cache_key}")
        return {"period": period, "top_backers": json.loads(cached), "source": "cache"}

    print(f"Cache miss. Querying Cassandra for period: {period}")
    query = "SELECT * FROM top_backers WHERE period = %s LIMIT %s"
    rows = session.execute(query, [period, limit])
    results = list(rows)

    r.setex(cache_key, 600, json.dumps(results))
    return {"period": period, "top_backers": results, "source": "database"}


# ==========================================
# SERVER RUNNER
# ==========================================
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)