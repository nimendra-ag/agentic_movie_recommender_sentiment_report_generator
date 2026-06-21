from playwright.sync_api import sync_playwright
import json
import time

def scrape_reddit():
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False
        )

        page = browser.new_page()

        # Open subreddit
        page.goto(
            "https://www.reddit.com/r/movies/",
            timeout=60000
        )

        page.wait_for_timeout(8000)

        # Scroll to load more posts
        for _ in range(5):
            page.mouse.wheel(0, 4000)
            page.wait_for_timeout(2000)

        # Collect post URLs first
        posts = page.query_selector_all("shreddit-post")

        post_data = []

        for post in posts[:10]:
            try:
                title = post.get_attribute("post-title")
                permalink = post.get_attribute("permalink")

                if title and permalink:
                    post_data.append({
                        "title": title,
                        "url": "https://www.reddit.com" + permalink
                    })

            except Exception as e:
                print("Post extraction error:", e)

        print("Posts collected:", len(post_data))

        # Visit each post
        for idx, post in enumerate(post_data, start=1):

            print(f"\n[{idx}] {post['title']}")

            try:
                page.goto(
                    post["url"],
                    timeout=60000
                )

                page.wait_for_timeout(8000)

                # Scroll comments into view
                for _ in range(5):
                    page.mouse.wheel(0, 5000)
                    page.wait_for_timeout(1500)

                comments = []

                # Reddit comments
                comment_elements = page.locator("shreddit-comment")

                comment_count = comment_elements.count()

                print("Comment elements:", comment_count)

                for i in range(comment_count):
                    try:
                        comment = comment_elements.nth(i)

                        paragraphs = comment.locator("p")

                        text_parts = []

                        for j in range(paragraphs.count()):
                            txt = paragraphs.nth(j).inner_text().strip()

                            if txt:
                                text_parts.append(txt)

                        comment_text = " ".join(text_parts)

                        if comment_text:
                            comments.append(comment_text)

                    except Exception:
                        pass

                results.append({
                    "title": post["title"],
                    "url": post["url"],
                    "comments": comments
                })

                time.sleep(2)

            except Exception as e:
                print("Error:", e)

        browser.close()

    return results


if __name__ == "__main__":

    data = scrape_reddit()

    with open(
        "reddit_posts_with_comments.json",
        "w",
        encoding="utf-8"
    ) as f:
        json.dump(
            data,
            f,
            indent=2,
            ensure_ascii=False
        )

    print("\nSaved to reddit_posts_with_comments.json")