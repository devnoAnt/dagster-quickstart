import json
import requests

import pandas as pd

from dagster import (
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_quickstart.configurations import HNStoriesConfig


@asset
def hackernews_top_story_ids(config: HNStoriesConfig):
    """Get top stories from the HackerNews top stories endpoint."""
    top_story_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()

    with open(config.hn_top_story_ids_path, "w") as f:
        json.dump(top_story_ids[: config.top_stories_limit], f)


@asset(deps=[hackernews_top_story_ids])
def hackernews_top_stories(config: HNStoriesConfig) -> MaterializeResult:
    """Get items based on story ids from the HackerNews items endpoint."""
    with open(config.hn_top_story_ids_path, "r") as f:
        hackernews_top_story_ids = json.load(f)

    results = []
    for item_id in hackernews_top_story_ids:
        item = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json").json()
        results.append(item)

    df = pd.DataFrame(results)
    df.to_csv(config.hn_top_stories_path)

    return MaterializeResult(
        metadata={
            "num_records": len(df),
            "preview": MetadataValue.md(str(df[["title", "by", "url"]].to_markdown())),
        }
    )


from dagster import asset
from dagster_postgres import PostgresResource
import pandas as pd

@asset(required_resource_keys={"postgres"})
def my_postgres_asset(context) -> pd.DataFrame:
    # Use the PostgresResource to execute a query and fetch data
    query = "SELECT * FROM evno.repositories"
    with context.resources.postgres.get_connection() as conn:
        return pd.read_sql(query, conn)

python
from dagster import repository
from dagster_postgres import postgres_resource

@repository
def my_repository():
    return [
        my_postgres_asset,
        # Define the postgres resource with the connection information
        postgres_resource.configured({"username": "evno_user", "password": "!5Eur@36Vn0s", "hostname": "dev-evno-rds.ccsyy8ftbuta.us-east-1.rds.amazonaws.com", "db_name": "postgres"}),
    ]