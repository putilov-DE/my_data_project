from fastapi import FastAPI, HTTPException
import redis
import pymysql
import json

# Initialize the application
app = FastAPI(title="AdTech Analytics API")

# Connect to Redis (inside Docker it is accessible by the container name 'redis')
redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)


# Connect to MySQL (connects out to your local machine via host.docker.internal)
def get_db_connection():
    return pymysql.connect(
        host='host.docker.internal',
        user='root',
        password='rootpassword',
        database='engineering_db',
        cursorclass=pymysql.cursors.DictCursor
    )


# ==========================================
# ENDPOINT 1: Campaign Performance (TTL = 30 seconds)
# ==========================================
@app.get("/campaign/{campaign_id}/performance")
def get_campaign_performance(campaign_id: int):
    cache_key = f"campaign_{campaign_id}_perf"

    # 1. Check the cache
    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    connection = None  # Prevent UnboundLocalError
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            sql = """
               SELECT 
                    COUNT(event_id) as total_impressions, 
                    SUM(CASE WHEN ad_revenue > 0 THEN 1 ELSE 0 END) as total_clicks, 
                    SUM(ad_cost) as total_spend
                FROM events 
                WHERE campaign_id = %s
            """
            cursor.execute(sql, (campaign_id,))
            result = cursor.fetchone()

            if not result or result['total_impressions'] is None:
                raise HTTPException(status_code=404, detail="Campaign not found")

            # Calculate CTR
            clicks = int(result['total_clicks'] or 0)
            impressions = int(result['total_impressions'] or 0)
            result['ctr'] = round((clicks / impressions) * 100, 2) if impressions > 0 else 0

            # 3. Save to cache for 30 seconds
            redis_client.set(cache_key, json.dumps(result, default=str), ex=30)
            return result
    finally:
        if connection:  # Close only if the connection was successful
            connection.close()


# ==========================================
# ENDPOINT 2: Advertiser Spending (TTL = 300 seconds / 5 minutes)
# ==========================================
@app.get("/advertiser/{advertiser_id}/spending")
def get_advertiser_spending(advertiser_id: int):
    cache_key = f"advertiser_{advertiser_id}_spend"

    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    connection = None  # Prevent UnboundLocalError
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            sql = """
                SELECT SUM(e.ad_cost) as total_spend
                FROM events e
                JOIN campaigns c ON e.campaign_id = c.campaign_id
                WHERE c.advertiser_id = %s
                """
            cursor.execute(sql, (advertiser_id,))
            result = cursor.fetchone()

            if not result or result['total_spend'] is None:
                raise HTTPException(status_code=404, detail="Advertiser not found")

            # Save to cache for 5 minutes
            redis_client.set(cache_key, json.dumps(result, default=str), ex=300)
            return result
    finally:
        if connection:  # Close only if the connection was successful
            connection.close()


# ==========================================
# ENDPOINT 3: User Engagements (TTL = 60 seconds)
# ==========================================
@app.get("/user/{user_id}/engagements")
def get_user_engagements(user_id: int):
    cache_key = f"user_{user_id}_engagements"

    cached_data = redis_client.get(cache_key)
    if cached_data:
        return json.loads(cached_data)

    connection = None  # Prevent UnboundLocalError
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            sql = """
                SELECT 
                    e.event_id, 
                    c.campaign_name, 
                    a.advertiser_name, 
                    e.event_timestamp as timestamp
                FROM events e
                JOIN campaigns c ON e.campaign_id = c.campaign_id
                JOIN advertisers a ON c.advertiser_id = a.advertiser_id
                WHERE e.user_id = %s AND e.ad_revenue > 0
            """
            cursor.execute(sql, (user_id,))
            result = cursor.fetchall()

            # Save to cache for 1 minute
            redis_client.set(cache_key, json.dumps(result, default=str), ex=60)
            return {"user_id": user_id, "engagements": result}
    finally:
        if connection:  # Close only if the connection was successful
            connection.close()