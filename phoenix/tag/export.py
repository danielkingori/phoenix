"""Export functionallity."""

import pandas as pd


def get_posts_to_scrape(posts_df) -> pd.DataFrame:
    """Get posts to scrape."""
    posts_to_scrape = posts_df[["page_name", "page_created", "total_interactions", "url"]]
    posts_to_scrape["url_to_scrape"] = posts_to_scrape["url"].str.replace(
        "https://www.facebook", "https://mbasic.facebook"
    )

    posts_to_scrape.sort_values(by="total_interactions", inplace=True, ascending=False)
    ten_percent = round(posts_to_scrape.shape[0] * 0.1)

    return posts_to_scrape[:ten_percent]
