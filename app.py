import json
import logging
import os
import re  # Added for HTML stripping
from datetime import datetime, timedelta
from typing import List, Dict, Any
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import feedparser
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from apscheduler.schedulers.background import BackgroundScheduler

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="News API")

# Config
MAX_NEWS_PER_CATEGORY = 200

# MongoDB setup (unchanged)
def get_mongo_client():
    if "MONGO_URI" not in os.environ:
        raise ValueError("MONGO_URI environment variable not set")
    return MongoClient(os.environ["MONGO_URI"])

client = None
db = None
def _init_mongo():
    global client, db
    if client is None:
        client = get_mongo_client()
        db = client["newsdb"]
        collection = db["news"]
        try:
            collection.count_documents({})
        except PyMongoError as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise HTTPException(status_code=500, detail="MongoDB connection failed")

# Countries, categories, fallback_queries (unchanged)
countries: Dict[str, Dict[str, str]] = {
    "us": {"gl": "US", "hl": "en-US", "lang": "en"},
    "gb": {"gl": "GB", "hl": "en-GB", "lang": "en"},
    "in": {"gl": "IN", "hl": "en-IN", "lang": "en"},
    "ca": {"gl": "CA", "hl": "en-CA", "lang": "en"},
    "au": {"gl": "AU", "hl": "en-AU", "lang": "en"},
    "ph": {"gl": "PH", "hl": "en", "lang": "en"},
    "za": {"gl": "ZA", "hl": "en", "lang": "en"},
    "ie": {"gl": "IE", "hl": "en-IE", "lang": "en"},
    "nz": {"gl": "NZ", "hl": "en-NZ", "lang": "en"},
    "sg": {"gl": "SG", "hl": "en", "lang": "en"},
}

categories: Dict[str, str] = {
    "all": None,
    "business": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGx6TVdZU0FtVnVHZ0pWVXlnQVAB",
    "science": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRFp0Y1RjU0FtVnVHZ0pWVXlnQVAB",
    "sports": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRFp1ZEdvU0FtVnVHZ0pWVXlnQVAB",
    "technology": "CAAqJggKIiBDQkFTRWdvSUwyMHZNRGRqTVhZU0FtVnVHZ0pWVXlnQVAB",
    "entertainment": "CAAqJggKIiBDQkFTRWdvSUwyMHZNREpxYW5RU0FtVnVHZ0pWVXlnQVAB",
    "health": "CAAqIQgKIhtDQkFTRGdvSUwyMHZNR3QwTlRFU0FtVnVLQUFQAQ",
    "politics": "CAAqIQgKIhtDQkFTRGdvSUwyMHZNRUZxY0dBU0FtVnVHZ0pWVWlnQVAB",
}

fallback_queries: Dict[str, str] = {
    "all": "",
    "business": "business",
    "science": "science",
    "sports": "sports",
    "technology": "technology",
    "entertainment": "entertainment",
    "health": "health",
    "politics": "politics",
}

class NewsItem(BaseModel):
    title: str
    date: str
    description: str
    url: str

def fetch_rss(rss_url: str) -> List[Dict[str, Any]]:
    try:
        feed = feedparser.parse(rss_url)
        if feed.bozo:
            logger.warning(f"RSS parse warning for {rss_url}: {feed.bozo_exception}")
        news = []
        seen_urls = set()
        for entry in (feed.entries or [])[:MAX_NEWS_PER_CATEGORY]:
            if entry.link in seen_urls:
                continue
            seen_urls.add(entry.link)
            date_str = datetime(*entry.published_parsed[:6]).isoformat() if "published_parsed" in entry else datetime.now().isoformat()

            # Improved description handling
            raw_desc = getattr(entry, "summary", getattr(entry, "description", ""))
            # Strip HTML tags
            clean_desc = re.sub(r'<[^>]+>', '', raw_desc).strip()
            # Truncate to ~200 chars for brevity; fallback to title if empty
            description = (clean_desc[:200] + "..." if clean_desc and len(clean_desc) > 200 else clean_desc) if clean_desc else entry.title

            news.append({
                "title": entry.title or "",
                "date": date_str,
                "description": description,
                "url": entry.link or ""
            })
        return news
    except Exception as e:
        logger.error(f"Failed to fetch/parse RSS from {rss_url}: {str(e)}")
        return []

# Rest of the code unchanged (update_news_doc, scrape_country_category, endpoints, etc.)
def update_news_doc(country: str, category: str, news: List[Dict[str, Any]]):
    if not news:
        logger.warning(f"No news to update for {country}/{category}")
        return
    _init_mongo()
    collection = db["news"]
    doc_id = f"{country}/{category}"
    try:
        existing_doc = collection.find_one({"_id": doc_id})
        existing_news = existing_doc["news"] if existing_doc else []

        # Filter old news (>1 year)
        one_year_ago = datetime.now() - timedelta(days=365)
        filtered_existing = []
        for item in existing_news:
            try:
                item_date = datetime.fromisoformat(item["date"])
                if item_date >= one_year_ago:
                    filtered_existing.append(item)
            except (ValueError, KeyError) as ve:
                logger.warning(f"Invalid date in existing news for {doc_id}: {item.get('date', 'N/A')} - {str(ve)}")
                continue

        # Dedup new (prevents duplicates based on URL)
        existing_urls = {item["url"] for item in filtered_existing if "url" in item}
        new_items = [item for item in news if item["url"] not in existing_urls]

        # Combine, sort desc, take max
        all_news = filtered_existing + new_items
        all_news.sort(key=lambda x: datetime.fromisoformat(x["date"]), reverse=True)
        all_news = all_news[:MAX_NEWS_PER_CATEGORY]

        # Upsert
        collection.replace_one(
            {"_id": doc_id},
            {"_id": doc_id, "news": all_news, "last_updated": datetime.now().isoformat()},
            upsert=True
        )
        logger.info(f"Updated {doc_id}: added {len(new_items)} new (duplicates skipped), retained {len(all_news)} total (max {MAX_NEWS_PER_CATEGORY})")
    except PyMongoError as pe:
        logger.error(f"MongoDB error updating {doc_id}: {str(pe)}")
        raise HTTPException(status_code=500, detail="Failed to update news in MongoDB")
    except Exception as e:
        logger.error(f"Unexpected error updating {doc_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Unexpected error updating news")

def scrape_country_category(country: str, category: str, country_info: Dict[str, str], topic_id: str):
    try:
        _init_mongo()
        gl, hl, lang = country_info["gl"], country_info["hl"], country_info["lang"]
        ceid = f"{gl}:{lang}"
        news = []
        used_fallback = False
        rss_url = None

        if topic_id is not None:
            rss_url = f"https://news.google.com/rss/topics/{topic_id}?hl={hl}&gl={gl}&ceid={ceid}"
            news = fetch_rss(rss_url)
            if not news:
                logger.info(f"Topic RSS failed for {country}/{category}, falling back to search")
                query_term = fallback_queries.get(category, category)
                if query_term:
                    rss_url = f"https://news.google.com/rss/search?q={quote(query_term)}&hl={hl}&gl={gl}&ceid={ceid}"
                    news = fetch_rss(rss_url)
                    used_fallback = True
        else:
            rss_url = f"https://news.google.com/rss?hl={hl}&gl={gl}&ceid={ceid}"
            news = fetch_rss(rss_url)

        if not news:
            logger.warning(f"No news fetched for {country}/{category} after fallback (URL: {rss_url})")
            return

        logger.info(f"Fetched {len(news)} items for {country}/{category} (fallback: {used_fallback})")
        update_news_doc(country, category, news)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scraping {country}/{category}: {str(e) or 'Unknown error'}")

# Scheduler
scheduler = BackgroundScheduler()
def scrape_all():
    logger.info("Starting scheduled scrape")
    errors = []
    for country, info in countries.items():
        for category, topic_id in categories.items():
            try:
                scrape_country_category(country, category, info, topic_id)
            except Exception as e:
                error_msg = str(e) or "Unknown error"
                errors.append(f"{country}/{category}: {error_msg}")
    if errors:
        logger.error(f"Scrape errors: {errors}")
    else:
        logger.info("Scrape completed")

@app.on_event("startup")
async def startup_event():
    try:
        _init_mongo()
        scheduler.add_job(scrape_all, "interval", minutes=10)
        scheduler.start()
        scrape_all()
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")

@app.get("/News/{country}/{category}", response_model=List[NewsItem])
async def get_news(
    country: str,
    category: str,
    page: int = Query(1, ge=1, description="Page number (1 = newest)"),
    per_page: int = Query(50, ge=1, le=200, description="Items per page (max 200)")
):
    if country not in countries or category not in categories:
        raise HTTPException(status_code=400, detail="Invalid country or category")
    _init_mongo()
    collection = db["news"]
    doc_id = f"{country}/{category}"
    try:
        doc = collection.find_one({"_id": doc_id})
        if not doc or not doc.get("news"):
            raise HTTPException(status_code=404, detail="No news data yet")
        
        total_items = len(doc["news"])
        skip = (page - 1) * per_page
        if skip >= total_items:
            return []
        paginated_news = doc["news"][skip:skip + per_page]
        
        total_pages = (total_items + per_page - 1) // per_page
        logger.info(f"Returned page {page} of {total_pages} for {doc_id} ({len(paginated_news)} items)")
        
        return [NewsItem(**item) for item in paginated_news]
    except PyMongoError as pe:
        logger.error(f"MongoDB error fetching {doc_id}: {str(pe)}")
        raise HTTPException(status_code=500, detail="Failed to retrieve news from MongoDB")
    except Exception as e:
        logger.error(f"Unexpected error fetching {doc_id}: {str(e)}")
        raise HTTPException(status_code=500, detail="Unexpected error retrieving news")

@app.get("/search")
async def search_news(
    query: str,
    page: int = Query(1, ge=1, description="Page number (1 = newest)"),
    per_page: int = Query(50, ge=1, le=200, description="Items per page (max 200)")
):
    if not query:
        raise HTTPException(status_code=400, detail="Query required")
    try:
        encoded_query = quote(query)
        rss_url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-US&gl=US&ceid=US:en"
        all_news = fetch_rss(rss_url)
        if not all_news:
            logger.warning(f"No search results for '{query}'")
            return []

        total_items = len(all_news)
        skip = (page - 1) * per_page
        if skip >= total_items:
            return []
        paginated_news = all_news[skip:skip + per_page]
        
        total_pages = (total_items + per_page - 1) // per_page
        logger.info(f"Search '{query}': returned page {page} of {total_pages} ({len(paginated_news)} items)")
        
        return JSONResponse(content=paginated_news)
    except Exception as e:
        logger.error(f"Search error for '{query}': {str(e)}")
        raise HTTPException(status_code=500, detail="Search failed")
