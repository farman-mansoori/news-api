import json
import logging
import os
import re
import time
import html  # Added for HTML entity decoding
import random  # Added for random delays
from datetime import datetime, timedelta
from typing import List, Dict, Any
from urllib.parse import quote
import difflib  # Added for similarity check

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import feedparser
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from apscheduler.schedulers.background import BackgroundScheduler
import requests
from requests.adapters import HTTPAdapter  # Added for retries
from urllib3.util.retry import Retry  # Added for retries
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="News API")

# Config
MAX_NEWS_PER_CATEGORY = 200
RSS_DESC_THRESHOLD = 100
SCRAPE_TIMEOUT = 10  # Increased timeout
MAX_SUMMARY_CHARS = 500
LAST_DAYS = 1  # Include news from last 1 day

# MongoDB setup
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

# Countries (unchanged)
countries: Dict[str, Dict[str, str]] = {
    "us": {"gl": "US", "hl": "en-US", "lang": "en"},
    "gb": {"gl": "GB", "hl": "en-GB", "lang": "en"},
    "in": {"gl": "IN", "hl": "en-IN", "lang": "en"},
    "ca": {"gl": "CA", "hl": "en-CA", "lang": "en"},
    "au": {"gl": "AU", "hl": "en-AU", "lang": "en"},
}

# Updated RSS feeds with verified working URLs
rss_feeds: Dict[str, Dict[str, str]] = {
    "us": {
        "top": "https://news.yahoo.com/rss",
        "business": "https://news.yahoo.com/rss/business",
        "science": "https://news.yahoo.com/rss/science",
        "sports": "https://news.yahoo.com/rss/sports",
        "technology": "https://news.yahoo.com/rss/technology",
        "entertainment": "https://news.yahoo.com/rss/entertainment",
        "health": "https://news.yahoo.com/rss/health",
        "politics": "https://news.yahoo.com/rss/politics",
    },
    "gb": {
        "top": "https://feeds.bbci.co.uk/news/rss.xml",
        "business": "https://feeds.bbci.co.uk/news/business/rss.xml",
        "science": "https://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
        "sports": "https://feeds.bbci.co.uk/sport/rss.xml",
        "technology": "https://feeds.bbci.co.uk/news/technology/rss.xml",
        "entertainment": "https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml",
        "health": "https://feeds.bbci.co.uk/news/health/rss.xml",
        "politics": "https://feeds.bbci.co.uk/news/politics/rss.xml",
    },
    "in": {
        "top": "https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
        "business": "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
        "science": "https://timesofindia.indiatimes.com/rssfeeds/2128805.cms",
        "sports": "https://timesofindia.indiatimes.com/rssfeeds/4719148.cms",
        "technology": "https://timesofindia.indiatimes.com/rssfeeds/66949542.cms",
        "entertainment": "https://timesofindia.indiatimes.com/rssfeeds/1081479906.cms",
        "health": "https://timesofindia.indiatimes.com/rssfeeds/10672306.cms",
        "politics": "https://timesofindia.indiatimes.com/rssfeeds/3835328.cms",
    },
    "ca": {
        "top": "https://www.cbc.ca/webfeed/rss/rss-topstories",
        "business": "https://www.cbc.ca/webfeed/rss/rss-business",
        "science": "https://www.cbc.ca/news/science/feed",
        "sports": "https://www.cbc.ca/webfeed/rss/rss-sports",
        "technology": "https://www.cbc.ca/webfeed/rss/rss-technology",
        "entertainment": "https://www.cbc.ca/webfeed/rss/rss-arts",
        "health": "https://www.cbc.ca/webfeed/rss/rss-health",
        "politics": "https://www.cbc.ca/webfeed/rss/rss-politics",
    },
    "au": {
        "top": "https://www.abc.net.au/news/feed/51120/rss.xml",
        "business": "https://www.abc.net.au/news/business/rss.xml",
        "science": "https://www.abc.net.au/news/science/rss.xml",
        "sports": "https://www.abc.net.au/news/sport/rss.xml",
        "technology": "https://www.abc.net.au/news/technology/rss.xml",
        "entertainment": "https://www.abc.net.au/news/arts/rss.xml",
        "health": "https://www.abc.net.au/news/health/rss.xml",
        "politics": "https://www.abc.net.au/news/politics/rss.xml",
    }
}

class NewsItem(BaseModel):
    title: str
    date: str
    source: str
    url: str
    image_url: str = ""
    description: str = ""

# Response wrapper model (unchanged)
class NewsResponse(BaseModel):
    status: str
    page: int
    per_page: int
    total_results: int
    articles: List[Dict[str, Any]]

def extract_source_from_rss(entry) -> str:
    """Extract source from RSS entry."""
    try:
        # Try different possible source fields in RSS
        if hasattr(entry, 'source') and entry.source:
            return getattr(entry.source, 'title', 'Unknown Source')
        elif hasattr(entry, 'author'):
            return entry.author
        # For Google News RSS, source is often in the description
        elif hasattr(entry, 'summary'):
            soup = BeautifulSoup(entry.summary, 'html.parser')
            font_tag = soup.find('font', color="#6f6f6f")
            if font_tag:
                return font_tag.get_text(strip=True)
    except Exception as e:
        logger.warning(f"Failed to extract source from RSS: {str(e)}")
    return "Unknown Source"

def scrape_article_details(url: str) -> tuple[str, str]:
    """Scrape image URL and description from the actual news article URL."""
    image_url = ""
    description = ""
    
    try:
        session = requests.Session()
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://news.google.com/',  # Added to mimic Google News
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        resp = session.get(url, headers=headers, timeout=SCRAPE_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        
        # Extract image URL
        og_image = soup.find('meta', {'property': 'og:image'})
        if og_image and og_image.get('content'):
            image_url = og_image['content']
        else:
            # Fallback: look for the first large image in article content
            article = soup.find('article') or soup.find('div', class_=re.compile(r'content|body|article|main'))
            if article:
                img = article.find('img')
                if img and img.get('src'):
                    image_url = img['src']
        
        # Extract description
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        if meta_desc and meta_desc.get('content'):
            description = re.sub(r'<[^>]+>', '', meta_desc['content']).strip()
            description = html.unescape(description)
        else:
            # Fallback: extract first few paragraphs
            article = soup.find('article') or soup.find('div', class_=re.compile(r'content|body|article|main|post|story'))
            if article:
                paras = article.find_all('p')[:3]
                full_text = ' '.join([p.get_text(strip=True) for p in paras if p.get_text(strip=True)])
                clean_text = re.sub(r'\s+', ' ', full_text).strip()
                description = html.unescape(clean_text)
        
        # Limit description length
        if len(description) > MAX_SUMMARY_CHARS:
            description = description[:MAX_SUMMARY_CHARS] + "..."
            
    except Exception as e:
        logger.warning(f"Failed to scrape article details for {url}: {str(e)}")
    
    return image_url, description

def enhance_news_with_article_details(news_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enhance RSS news items with details scraped from actual article URLs only if missing from RSS."""
    enhanced_news = []
    scraped_count = 0
    
    for item in news_items:
        url = item.get("url", "")
        current_image = item.get("image_url", "").strip()
        current_desc = item.get("description", "").strip()
        
        # Only scrape if both image and description are missing or empty
        needs_scrape = not current_image and not current_desc
        
        if url and needs_scrape:
            logger.info(f"Scraping details (missing from RSS) for: {item['title'][:50]}...")
            scraped_image, scraped_desc = scrape_article_details(url)
            
            # Use scraped data if available
            if scraped_image:
                item["image_url"] = scraped_image
            if scraped_desc:
                item["description"] = scraped_desc
            scraped_count += 1
            
            # Rate limiting
            time.sleep(random.uniform(0.5, 1.5))  # Random delay
        else:
            if needs_scrape:
                logger.info(f"Skipping scrape for {item['title'][:50]}... (has partial RSS data)")
            else:
                logger.debug(f"Full RSS data available for {item['title'][:50]}...")
        
        enhanced_news.append(item)
    
    logger.info(f"Scraped {scraped_count} articles (out of {len(news_items)}) where data was missing from RSS")
    return enhanced_news

def fetch_rss(rss_url: str) -> List[Dict[str, Any]]:
    """Fetch and parse RSS feed with proper headers, retries, and date filtering."""
    try:
        # Get cutoff date for filtering
        cutoff_date = datetime.now().date() - timedelta(days=LAST_DAYS)
        
        # Enhanced headers to mimic a real browser (updated User-Agent)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://news.google.com'  # Added to appear more legitimate
        }
        
        # Setup session with retries for 503/504 errors (exponential backoff)
        session = requests.Session()
        retry_strategy = Retry(
            total=3,  # Max 3 retries
            backoff_factor=2,  # Wait 2s, 4s, 8s between retries
            status_forcelist=[403, 503, 504],  # Added 403
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        response = session.get(rss_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        # Check if the response is actually RSS/XML
        content_type = response.headers.get('content-type', '').lower()
        if 'xml' not in content_type and 'rss' not in content_type:
            logger.warning(f"Unexpected content type for {rss_url}: {content_type}")
        
        # Parse the content with feedparser
        feed = feedparser.parse(response.content)
        
        if feed.bozo:
            logger.warning(f"RSS parse warning for {rss_url}: {feed.bozo_exception}")
        
        news = []
        seen_urls = set()
        
        for entry in (feed.entries or [])[:MAX_NEWS_PER_CATEGORY]:
            if not hasattr(entry, 'link') or not entry.link:
                continue
                
            if entry.link in seen_urls:
                continue
            seen_urls.add(entry.link)
            
            # Extract basic fields from RSS
            title = getattr(entry, 'title', '') or ""
            url = entry.link
            
            # Parse and validate date - only include recent news
            entry_date = None
            date_str = datetime.now().isoformat()  # Default to current time
            
            # Try different date fields
            date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
            for field in date_fields:
                if hasattr(entry, field) and getattr(entry, field):
                    try:
                        entry_date = datetime(*getattr(entry, field)[:6])
                        date_str = entry_date.isoformat()
                        break
                    except (TypeError, ValueError):
                        continue
            
            # Filter by date - include last LAST_DAYS days
            if entry_date and entry_date.date() < cutoff_date:
                continue
            
            source = extract_source_from_rss(entry)
            
            # Extract description from RSS
            description = ""
            if hasattr(entry, 'summary') and entry.summary:
                soup = BeautifulSoup(entry.summary, 'html.parser')
                text = soup.get_text()
                description = re.sub(r'\s+', ' ', text).strip()
                if len(description) > MAX_SUMMARY_CHARS:
                    description = description[:MAX_SUMMARY_CHARS] + "..."
            elif hasattr(entry, 'description') and entry.description:
                description = entry.description.strip()
                if len(description) > MAX_SUMMARY_CHARS:
                    description = description[:MAX_SUMMARY_CHARS] + "..."
            
            # Extract image from RSS
            image_url = ""
            if hasattr(entry, 'enclosures'):
                for enclosure in entry.enclosures:
                    if 'image' in enclosure.get('type', ''):
                        image_url = enclosure.get('href', '')
                        break
            if not image_url and hasattr(entry, 'media_content'):
                for media in entry.media_content or []:
                    if media.get('medium') == 'image':
                        image_url = media.get('url', '')
                        break
            # Additional check for media_thumbnail
            if not image_url and hasattr(entry, 'media_thumbnail'):
                image_url = entry.media_thumbnail[0].get('url', '') if entry.media_thumbnail else ""
            
            # Create news item with RSS details
            news_item = {
                "title": title,
                "date": date_str,
                "source": source,
                "url": url,
                "image_url": image_url,
                "description": description
            }
            
            news.append(news_item)
        
        logger.info(f"Successfully parsed {len(news)} items from {rss_url} (last {LAST_DAYS} days)")
        return news
        
    except requests.RequestException as e:
        logger.error(f"Request failed for {rss_url}: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"Failed to fetch/parse RSS from {rss_url}: {str(e)}")
        return []

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

        # Only keep news from last LAST_DAYS
        cutoff_date = datetime.now().date() - timedelta(days=LAST_DAYS)
        filtered_existing = []
        for item in existing_news:
            try:
                item_date = datetime.fromisoformat(item["date"]).date()
                if item_date >= cutoff_date:
                    filtered_existing.append(item)
            except (ValueError, KeyError) as ve:
                logger.warning(f"Invalid date in existing news for {doc_id}: {item.get('date', 'N/A')} - {str(ve)}")
                continue

        existing_urls = {item["url"] for item in filtered_existing if "url" in item}
        new_items = [item for item in news if item["url"] not in existing_urls]

        all_news = filtered_existing + new_items
        all_news.sort(key=lambda x: datetime.fromisoformat(x["date"]), reverse=True)
        all_news = all_news[:MAX_NEWS_PER_CATEGORY]

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

def scrape_country_category(country: str, category: str, country_info: Dict[str, str]):
    try:
        _init_mongo()
        if country not in rss_feeds or category not in rss_feeds[country]:
            logger.warning(f"No RSS feed configured for {country}/{category}")
            return
        
        rss_url = rss_feeds[country][category]

        # Add random delay (1-3s) before each RSS fetch to stagger requests and avoid bursts
        time.sleep(random.uniform(1, 3))

        news = fetch_rss(rss_url)

        if not news:
            logger.warning(f"No news fetched for {country}/{category} (URL: {rss_url})")
            return

        logger.info(f"Fetched {len(news)} basic items for {country}/{category}, now enhancing missing details...")
        
        # Enhance with details from actual article URLs only if needed
        enhanced_news = enhance_news_with_article_details(news)
        
        # Add country/category to each item for storage
        for item in enhanced_news:
            item["country"] = country
            item["category"] = category
            
        update_news_doc(country, category, enhanced_news)
        logger.info(f"Completed enhancing for {len(enhanced_news)} articles")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error scraping {country}/{category}: {str(e) or 'Unknown error'}")

scheduler = BackgroundScheduler()
def scrape_all():
    logger.info("Starting scheduled scrape")
    errors = []
    for country, info in countries.items():
        for category in rss_feeds.get(country, {}).keys():
            try:
                scrape_country_category(country, category, info)
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
        scheduler.add_job(scrape_all, "interval", minutes=30)
        scheduler.start()
        scrape_all()
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    try:
        _init_mongo()  # Test MongoDB connection
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Service unhealthy")

@app.get("/News/{country}/{category}", response_model=NewsResponse)
async def get_news(
    country: str,
    category: str,
    page: int = Query(1, ge=1, description="Page number (1 = newest)"),
    per_page: int = Query(50, ge=1, le=200, description="Items per page (max 200)")
):
    if country not in countries or category not in rss_feeds.get(country, {}):
        raise HTTPException(status_code=400, detail="Invalid country or category")
    _init_mongo()
    collection = db["news"]
    doc_id = f"{country}/{category}"
    try:
        doc = collection.find_one({"_id": doc_id})
        if not doc or not doc.get("news"):
            raise HTTPException(status_code=404, detail="No news data yet")
        
        all_articles = doc["news"]
        total_results = len(all_articles)
        skip = (page - 1) * per_page
        if skip >= total_results:
            articles = []
        else:
            articles = all_articles[skip:skip + per_page]
            # Ensure country/category are in each article (from storage)
            for article in articles:
                if "country" not in article:
                    article["country"] = country
                if "category" not in article:
                    article["category"] = category
        
        response_data = {
            "status": "ok",
            "page": page,
            "per_page": per_page,
            "total_results": total_results,
            "articles": articles
        }
        total_pages = (total_results + per_page - 1) // per_page
        logger.info(f"Returned page {page} of {total_pages} for {doc_id} ({len(articles)} items)")
        
        return NewsResponse(**response_data)
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
        # Use Bing News RSS search (RSS-only). Safer than scraping Google.
        encoded_query = quote(query)
        rss_url = f"https://www.bing.com/news/search?q={encoded_query}&format=rss"

        basic_articles = fetch_rss(rss_url)
        if not basic_articles:
            logger.warning(f"No search results for '{query}'")
            response_data = {
                "status": "ok",
                "page": page,
                "per_page": per_page,
                "total_results": 0,
                "articles": []
            }
            return JSONResponse(content=response_data)

        all_articles = basic_articles
        total_results = len(all_articles)
        skip = (page - 1) * per_page
        if skip >= total_results:
            articles = []
        else:
            articles = all_articles[skip:skip + per_page]
            for article in articles:
                article["country"] = article.get("country", "us")
                article["category"] = article.get("category", "general")

        response_data = {
            "status": "ok",
            "page": page,
            "per_page": per_page,
            "total_results": total_results,
            "articles": articles
        }
        logger.info(f"Search '{query}': returned page {page} ({len(articles)} items)")
        return JSONResponse(content=response_data)
    except Exception as e:
        logger.error(f"Search error for '{query}': {str(e)}")
        raise HTTPException(status_code=500, detail="Search failed")
