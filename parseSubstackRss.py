import os
import hashlib
from datetime import datetime
from pathlib import Path
import json
import requests
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from bs4 import BeautifulSoup as soup, Comment
from collections import defaultdict
import os
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi  

prod = False
load_dotenv()

def fetch_rss_data(url):
    """
    Fetches data from the given URL and prints the raw data.
    
    Args:
        url (str): The URL to fetch data from
    """
    print(f"Fetching data from: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/rss+xml, application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raises an exception for bad status codes
        print(f"Content-Length header: {response.headers.get('Content-Length', 'Unknown')} bytes")
        print(f"Actual response size: {len(response.content)} bytes")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise


def find_last_rss_article_date(rss_data):
    items = rss_data.findall("./channel/item")
    print("Number of items:", len(items))
    first_item = rss_data.find("./channel/item")
    title = first_item.findtext("title")
    pub_date = first_item.findtext("pubDate")
    try:
        pub_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")
        print("Blog Post:", title, "was published on Date:", pub_date)
        return pub_date
    except ValueError as e:
        print("Error parsing most recent rss date", (pub_date), ":", e)
        raise


def download_image(image_url, output_filename):
    """
    Download an image from a URL and save it locally.
    
    Args:
        image_url (str): URL of the image to download
        output_filename (str): Local filename to save the image as
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(image_url, headers=headers, stream=True)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Error downloading image", (image_url), ":", e)
        return None
    else:
        try:
            # Create images directory if it doesn't exist
            os.makedirs('images', exist_ok=True)
            # Save the image
            with open(f'images/{output_filename}', 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Downloaded image to: images/{output_filename}")
            return f"images/{output_filename}"
        except Exception as e:
            print(f"Error downloading image: {e}")
            raise


def generate_hashed_filename(path, length=10):
    """
    Given an image path, return a new filename with a short hash appended.
    """
    try:
        p = Path(path)
        # Compute hash of file contents (SHA1 in this example)
        with open(p, "rb") as f:
            file_bytes = f.read()
            digest = hashlib.sha1(file_bytes).hexdigest()[:length]
    except Exception as e:
        print(f"Error generating hashed filename: {e}")
        return None
    else:
        # Build new name: original stem + '-' + hash + extension
        return f"{p.stem}-{digest}{p.suffix}"


def find_new_posts(rss_data, mongo_date, site_url, db_name):
    new_data = []
    items = rss_data.findall("./channel/item")
    # ns = {"content": "http://purl.org/rss/1.0/modules/content/"} delete me?
    for post in items:
        try:
            pub_date = post.findtext("pubDate")
            pub_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z")
        except Exception as e:
            print("Error parsing pubDate", (pub_date), ":", e)
            raise

        if not compare_dates(mongo_date, pub_date):
            title = post.findtext("title")
            link = post.findtext("link")
            description = post.findtext("description")
            
            
            image_local = "" # set default values
            
            # Handle enclosure tag for images
            try:
                enclosure = post.find('enclosure')
                if enclosure is not None and 'url' in enclosure.attrib:
                    image_url = enclosure.attrib['url']
                    # Create a filename from the post title
                    safe_title = title.replace("-", "").replace("  ", " ")
                    safe_title = "".join(c.lower() if c.isalnum() else "-" for c in safe_title)
                    safe_title = safe_title.replace("--", "-")
                    if safe_title[-1] == '-':
                        safe_title = safe_title[:-1]
                    image_extension = image_url.split('.')[-1].split('?')[0]  # Remove query params
                    image_filename = f"{safe_title}.{image_extension}"
                    # Download the image
                    local_image_path = download_image(image_url, image_filename)
                    hashed_name = generate_hashed_filename(local_image_path)
                    if not prod:
                        try:
                            os.rename(local_image_path, "./images/" + hashed_name)
                        except Exception as e:
                            print("Error renaming image", (local_image_path), "to", (hashed_name), ":", e)
                    image_local = "/images/" + db_name + "/" + hashed_name
            except Exception as e:
                print("Error parsing xml for rss image:", e)
                raise

            try:
                likes, comments, name, blog_id = retrieve_html_data(link, site_url)
            except Exception as e:
                print("Error getting engagement data", (link), ":", e)
                raise
            
            if blog_id == None:
                print("Blog has no idea, so skipping")
                continue
            # Format the post data
            post_data = {
                "title": title,
                "id": int(blog_id) if blog_id and blog_id.isdigit() else 0,
                "link": link,
                "subtitle": description,
                "image_local": image_local,
                "name": name or "Jack T. Belshe",
                "reaction_count": int(likes) if likes and str(likes).isdigit() else 0,
                "comment_count": int(comments) if comments and str(comments).isdigit() else 0,
                "content_date": pub_date
            }
            
            
            new_data.append(post_data)
        else:
            break
    return new_data # can possible be empty


def retrieve_html_data(html_url, base_url):
    """
    Fetches HTML from a URL and prints it with <head> and <script> tags removed.
    
    Args:
        url (str): The URL to fetch HTML from
    """
    # Set headers to avoid 403 errors
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    try:
        # Make the request
        response = requests.get(html_url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print("Error fetching HTML", (html_url), ":", e)
        raise

    try:
        soup_obj = soup(response.text, 'html.parser') # Parse the HTML
        
        
        if soup_obj.head:  # Remove <head> tag
            soup_obj.head.decompose()
        for script in soup_obj.find_all('script'): # Remove all <script> tags
            script.decompose()
        
        for comment in soup_obj.find_all(string=lambda text: isinstance(text, Comment)): # Remove comments
            comment.extract()
            
        # Find and print like button containers
        like_buttons = soup_obj.find_all(class_=lambda c: c and 'like-button-container' in c.split())
        # print("\nFound", len(like_buttons), "like button containers:")
        for i, like_button in enumerate(like_buttons, 1):
            # print(f"\n--- Like Button {i} ---")
            like_count = like_button.get_text(strip=True)
            
        # Find and print comment buttons
        comment_buttons = soup_obj.find_all('a', class_=lambda c: c and 'post-ufi-comment-button' in c.split())
        # print("\n\nFound", len(comment_buttons), "comment buttons:")
        for i, comment_button in enumerate(comment_buttons, 1):
            comment_count = comment_button.get_text(strip=True)
            
        # Find profile name from profile hover card
        profile_div = soup_obj.find('div', class_=lambda c: c and 'profile-hover-card-target' in c.split())
        if profile_div:
            profile_name = profile_div.get_text(strip=True)
            # print("Profile Name:", profile_name)
        
        # Find and parse specific data-href values
        href_buttons = soup_obj.find_all(attrs={"data-href": True})
        id_count = defaultdict(int)
        for i, button in enumerate(href_buttons, 1):
            data_href = button.get('data-href', '')
            url = base_url + "/i/"
            if data_href.startswith(url):
                # Extract the part after /i/ and before the next /
                parts = data_href.split('/i/')
                if len(parts) > 1:
                    section_id = parts[1].split('/')[0]
                    id_count[section_id] += 1
        if len(id_count) == 0:
            raise Exception("No blog ID found")
        blog_id = max(id_count, key=id_count.get)
        
        return like_count, comment_count, profile_name, blog_id
        
    except Exception as e:
        print(f"Error parsing HTML {html_url} for likes, comments, profile name, and blog ID: {e}")
        raise


def compare_dates(mongo_date, rss_date) -> int:
    """
    Compare saved_date and last_post_date and return True if they are the same.
    
    Args:
        saved_date (str): The previously saved date
        last_post_date (str): The date from the latest RSS item
    
    Returns:
        bool: True if dates are the same, False otherwise
    """
    print("Comparing:", mongo_date, "to", rss_date)
    if mongo_date is None or rss_date is None:
        print("One or both dates are None")
        return True

    # Compare the dates directly
    if mongo_date >= rss_date:
        print("Dates match or DB date is newer - no new post")
        return True
    else:  # if the saved date is older than the other post's date
        print("Dates don't match - new post available")
        print(f"Saved date: {mongo_date}")
        print(f"Latest post date: {rss_date}")
        return False


def mongo_get_most_recent_date(db, db_name):
    # Create a new client and connect to the server
    print("Getting most recent date for", db_name)
    collection = db[db_name]
    try:
        pipeline = [
            {"$unwind": "$data"},                                # flatten the data array
            {"$sort": {"data.content_date": -1}},                # sort by newest first
            {"$limit": 1},                                       # only keep the top one
            {"$project": {"_id": 0, "latest_date": "$data.content_date"}}
        ]
        result = collection.aggregate(pipeline)
        latest = next(result, None)
        if latest:
            print("Most recent content_date:", latest["latest_date"])
            return latest["latest_date"]
    
        
        # if doc and "data" in doc:
        #     posts = doc["data"]
        #     for i, post in enumerate(posts):
        #         #print(doc)
        #         val = post.get("content_date")
        #         if isinstance(val, str):
        #             try:
        #                 dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
        #                 dt = dt.replace(microsecond=0, tzinfo=None)
        #             except Exception:
        #                 dt = datetime.strptime(val, "%a, %d %b %Y %H:%M:%S %Z")
        #             print(dt)
        #             collection.update_one(
        #                 {"_id": doc["_id"]},
        #                 {"$set": {f"data.{i}.content_date": dt}}
        #             )
                # posts.update_one({"_id": doc["_id"]}, {"$set": {"content_date": dt}})
    except Exception as e:
        print("ERROR in Mongo Most Recent Retrieval:", e)


def mongo_post_new_posts(db, db_name, new_posts):
    collection = db[db_name]
    doc = collection.find_one()
    posts = doc["data"]
    try:
        for post in new_posts:
            exists = collection.find_one({"data.id": post["id"]}, {"_id": 1})
            if exists:
                print("Duplicate id found, skipping insert.")
            else:
                print("Unique id, safe to append.")
                posts.append(post)
                collection.update_one({"_id": doc["_id"]}, {"$set": {"data": posts}})
    except Exception as e:
        print("ERROR in Mongo Post New Posts:", e)



def main():
    try:
        urls = ['https://jtbelshe.substack.com', 'https://technoblogic.substack.com'] 
        mongo_dbs = ['the-great-escape', 'techno-blogic'] 
        try:
            uri = os.environ.get("MONGO_URI")
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,   # fail fast if unreachable
                retryWrites=True,
                server_api=ServerApi('1')
            )   
            my_db_name = os.environ.get("DB_NAME")
            db = client[my_db_name] 
        except Exception as e:
            print("ERROR in Mongo Connection:", e)
            raise
        
        for url, db_name in zip(urls, mongo_dbs):
            print(f"Starting Substack RSS processor for {url}")
            # find the most recent post from the mongo db
            mongo_date = datetime.strptime("2025-09-17 09:45:10", "%Y-%m-%d %H:%M:%S") # mongo_get_most_recent_date(db, db_name)
            try:
                # Fetch and parse RSS feed
                rss_text = fetch_rss_data(url + "/feed")
                if not rss_text:
                    raise Exception("Failed to fetch RSS feed date, return value was None")
                    
                rss_data = ET.fromstring(rss_text)
                
                # Find the most recent post from the rss feed
                latest_post_date = find_last_rss_article_date(rss_data)
                print("Most Recent Post for:", db_name, "is from:", latest_post_date)

                # Check if there there is a newer post in the rss feed than in DB
                if not mongo_date or not compare_dates(mongo_date, latest_post_date):
                    new_posts = find_new_posts(rss_data, mongo_date, url, db_name)
                    print("New posts found!  Adding", len(new_posts), "posts to", db_name)
                    mongo_post_new_posts(db, db_name, new_posts)
                else:
                    print("No new posts since last check")
                    
            except Exception as e:
                print(f"Aborting processing of {url} due to error: {e}")
                raise
    except Exception as e:
        print("ERROR in Main:", e)
        raise
    else:
        print("Substack RSS processor completed successfully")

if __name__ == "__main__":
    main()
