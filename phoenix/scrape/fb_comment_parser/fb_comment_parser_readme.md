# Facebook commment parser

## Note
For simplicity's sake I've just included everything, so you can see my approach. 

## Contents
- `main.py` - suite of functions that allow for reading and parsing of single files or directories.
- `soup_parser.py` - parsing script, outputs a `page` object.
- `date_parser.py` - parses facebooks human-readable dates, i.e. 'yesterday' or 'three days ago' and approximates the date.
  - *This has a bug where it sets the date as 00:00 and then converts to UTC, which had the effect of putting comments in the evening hours of the previous day*


## Use
`soup_parser.py` will take in raw html as a file and output a `Page` object with several features listed below, including possibly a list of `Comment` objects, also detailed below.

To see the formats:
```python
from soup_parser import Page
page_obj = Page(html_file)
page_obj.as_dict
page_obj.json
```

## To Do
- testing & more testing & unit tests
## Data organization outputs

### Pages
`page.as_dict` returns the following:
- `file_id`           : The name of the file as a unique ID for this batch of data.
- `title`             : title gathered from a header, but this is not reliable.
- `comments`          : list of Comment objects (see below).
- `type`              : type of page - story, replies, reactions, wall, something else.
- `page_id`           : id of page where this would be found (sometimes empty on replies or reactions, but it's ok).
- `top_post_owner`    : id of the owner of the original post where this is found (sometimes empty, but it's ok).
- `top_post_username` : Username of the top post owner.
- `parent_post`       : post id where this item is found - if it `== post_id`, it's a post, if it is a different post, a comment, and a different comment, a sub-comment.
- `post_id`           : the id for this specific item.
- `retrieved_utc`     : when it was retrived by SingleFile adjusted to UTC.
- `reactions`         : if `type == 'reactions'` this is a list of reactions (see below).

#### Comments
`comment.as_dict` returns the following:
-  `file_id`          : see `page.file_id` 
-  `date`             : date of post as found on FB when retrieved <_Tuesday at 3:27 PM_>
-  `date_utc`         : date of post normalized to UTC <_2020-07-07 17:27:48_>
-  `display_name`     : display name of the comment owner <_محمد عمر_>
-  `fb_id`            : Facebook id of the specific comment <_616111729289804_>
-  `parent`           : id of the object above (if it is the fb_id, it's a post) <_616103579290619_>
-  `reactions`        : number of reactions <_60_>
-  `sentiment`        : rough type of reactions reported in order from most to third-most <[_Like_, _Haha_, _Love_]>
-  `text`             : post texts <_رد سريع وقاسي الله يبارك_>
-  `top_level_post_id`: top level post <_616103579290619_>
-  `username`         : username for navigating facebook - could be number or string <_100001749670760_> <_jacob.lefton_>
- `images`            : a list of dictionaries containing image information if existing
- `connections`       : a list of tags of users or hashtags (see below)
- `position`          : is it a post, comment, or subcomment?

##### Images
`comment.image[<index>]` returns the following
- `src`               : Image encoded as jpeg base64 (possibly also png, though right now I'm filtering those)
- `alt`               : Alt text - usually machine learning guess of contents of image - valuable
- `width`             : image width in px
- `height`            : image height in px
- `class`             : class attribute used by FB - probably unnecessary

##### connections
`comment.connections` contains hashtags or users lists
- `hashtags`
    -   `tag_url`     : url format of hashtag used in `mbasic.facebook.com/hashtag/<tag_url>`
    -   `tag_text`    : display text of hashtag (eg. #blacklivesmatter)
- _users_
    - `username`      : same as comment record  - connects users by tag and conversation interaction or association
    - `display_name`  : display name of user **could be different from other, more complete records** because you can use just first name (though most don't)
- _links_
    - `text`          : text of linked content
    - `link`          : link to linked content
