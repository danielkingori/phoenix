"""Transform the output of comments parser into CrowdTangle data format ready for data pull."""
import numpy as np


def transform(pages: list) -> list:
    """Transform the parsed comments data, into posts data as per CrowdTangle format."""
    posts = []
    for page in pages:
        for comment_post_entry in page["comments"]:
            if not comment_post_entry["position"] == "post":
                continue
            else:
                post_dict = {}
                post_dict["date"] = comment_post_entry["date_utc"]
                post_dict["updated"] = comment_post_entry["date_utc"]
                post_dict["postUrl"] = (
                    f"https://www.facebook.com/{page['page']['top_post_owner']}"
                    f"/posts/{comment_post_entry['post_id']}"
                )
                post_dict["link"] = post_dict["postUrl"]
                if comment_post_entry["images"]:
                    post_dict["type"] = "photo"
                else:
                    post_dict["type"] = "status"
                post_dict["languageCode"] = "und"
                post_dict["platform"] = "Facebook"
                post_dict["message"] = comment_post_entry["text"]
                post_dict["account"] = {}
                post_dict["account"]["name"] = comment_post_entry["user_display_name"]
                post_dict["account"]["handle"] = comment_post_entry["user_name"]
                post_dict["account"][
                    "url"
                ] = f"https://www.facebook.com/{page['page']['top_post_owner']}"
                post_dict["account"]["platformId"] = page["page"]["top_post_owner"]
                for entry in [
                    "pageCategory",
                    "pageDescription",
                    "pageAdminTopCountry",
                    "pageCreatedDate",
                ]:
                    post_dict["account"][entry] = np.nan

                post_dict["score"] = comment_post_entry["reactions"]

                post_dict["statistics"] = {
                    "actual": {
                        "likeCount": np.nan,
                        "shareCount": np.nan,
                        "commentCount": np.nan,
                        "loveCount": np.nan,
                        "wowCount": np.nan,
                        "hahaCount": np.nan,
                        "sadCount": np.nan,
                        "angryCount": np.nan,
                        "thankfulCount": np.nan,
                        "careCount": np.nan,
                    }
                }

                posts.append(post_dict)
    return posts
