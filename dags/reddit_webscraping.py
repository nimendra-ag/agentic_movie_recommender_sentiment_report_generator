import json
import time
from datetime import datetime as dt, timedelta, timezone as tz

from pendulum import datetime, timezone
from playwright.sync_api import sync_playwright

from airflow.sdk import dag, task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.date_time import DateTimeSensor


SUBREDDIT = "moviereviews" #movies
LOOKBACK_DAYS = 4
MAX_PAGES = 5  # max pagination pages to check
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/137.0.0.0 Safari/537.36"
)


@dag(
    dag_id="reddit_webscraping",
    schedule="58 13 */2 * *",
    start_date=datetime(2026, 1, 1, tz=timezone("Asia/Colombo")),
    catchup=False,
    tags=["web_scraping", "reddit", "netflix"],
)
def reddit_webscraping_dag():

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    wait_sensor = DateTimeSensor(
        task_id="wait_sensor",
        target_time=(
            "{{ data_interval_end.in_timezone('Asia/Colombo')"
            ".replace(hour=14, minute=0, second=0) }}"
        ),
    )

    @task
    def create_cluster():
        # cluster = DataprocCreateClusterOperator()
        cluster = "cluster is created"
        return cluster

    @task
    def scrape_reviews():
        """Scrape only posts from the last LOOKBACK_DAYS days."""

        cutoff = dt.now(tz.utc) - timedelta(days=LOOKBACK_DAYS)
        print(f"Cutoff datetime (UTC): {cutoff.isoformat()}")

        results = []
        reached_old_posts = False

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )

            context = browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
            )

            page = context.new_page()

            # Start with newest posts
            next_url = f"https://old.reddit.com/r/{SUBREDDIT}/new/"

            for page_num in range(1, MAX_PAGES + 1):
                print(f"\n--- Page {page_num}: {next_url} ---")

                page.goto(next_url, timeout=60000)
                page.wait_for_timeout(5000)

                post_elements = page.query_selector_all("div.thing.link")
                print(f"Posts on page: {len(post_elements)}")

                if not post_elements:
                    break

                for el in post_elements:
                    try:
                        # Extract post timestamp
                        time_el = el.query_selector("time")
                        if not time_el:
                            continue

                        post_datetime_str = time_el.get_attribute("datetime")
                        if not post_datetime_str:
                            continue

                        # Parse ISO timestamp (e.g. 2026-06-20T10:30:00+00:00)
                        post_datetime = dt.fromisoformat(post_datetime_str)

                        # Skip posts older than cutoff
                        if post_datetime < cutoff:
                            print(
                                f"  Reached post from {post_datetime.isoformat()}"
                                f" — older than cutoff, stopping."
                            )
                            reached_old_posts = True
                            break

                        title_el = el.query_selector("a.title")
                        comments_el = el.query_selector("a.comments")

                        if not title_el or not comments_el:
                            continue

                        title = title_el.inner_text().strip()
                        comments_url = comments_el.get_attribute("href") or ""

                        if not comments_url.startswith("http"):
                            comments_url = f"https://old.reddit.com{comments_url}"

                        old_comments_url = comments_url.replace(
                            "www.reddit.com", "old.reddit.com"
                        )

                        results.append({
                            "title": title,
                            "url": comments_url.replace(
                                "old.reddit.com", "www.reddit.com"
                            ),
                            "comments_url": old_comments_url,
                            "posted_at": post_datetime.isoformat(),
                        })

                        print(f"  Collected: {title[:80]}")

                    except Exception as e:
                        print(f"  Post extraction error: {e}")

                if reached_old_posts:
                    break

                # Find the "next" pagination link
                next_btn = page.query_selector("span.next-button a")
                if next_btn:
                    next_url = next_btn.get_attribute("href") or ""
                    if not next_url:
                        break
                else:
                    break

            print(f"\nTotal posts within last {LOOKBACK_DAYS} days: {len(results)}")

            # ---- Fetch comments for each collected post ----
            for idx, post in enumerate(results, start=1):
                print(f"\n[{idx}] Fetching comments: {post['title'][:80]}")

                comments = []
                try:
                    page.goto(post["comments_url"], timeout=60000)
                    page.wait_for_timeout(5000)

                    comment_elements = page.query_selector_all(
                        "div.comment div.md"
                    )

                    for cel in comment_elements:
                        try:
                            text = cel.inner_text().strip()
                            if text:
                                comments.append(text)
                        except Exception:
                            pass

                    print(f"  Comments collected: {len(comments)}")

                except Exception as e:
                    print(f"  Error fetching comments: {e}")

                post["comments"] = comments
                # Remove the internal-only field
                post.pop("comments_url", None)

                time.sleep(2)

            browser.close()

        # Save results
        current_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
        output_path = f"/usr/local/airflow/include/reddit_scrape_{SUBREDDIT}_{current_time}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

        print(f"\nSaved {len(results)} posts to {output_path}")
        return output_path

    @task
    def delete_cluster():
        cluster = "cluster is deleted"
        return cluster

    create_cluster_task = create_cluster()
    scrape_reviews_task = scrape_reviews()
    delete_cluster_task = delete_cluster()
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline >> wait_sensor >> create_cluster_task >> scrape_reviews_task >> delete_cluster_task >> end_pipeline
    )


reddit_webscraping_dag()