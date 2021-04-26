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
- _ref_id_            : random generated reference id for this particular file
- _title_             : title gathered from a header, but this is not reliable
- _comments_          : list of Comment objects (see below)
- _type_              : type of page - story, replies, reactions, wall, something else
- _page_id_           : id of page where this would be found (sometimes empty on replies or reactions, but it's ok)
- _top_post_owner_    : id of the owner of the original post where this is found (sometimes empty, but it's ok)
- _top_post_username_ : Username of the top post owner
- _parent_post_       : post id where this item is found - if it `== post_id`, it's a post, if it is a different post, a comment, and a different comment, a sub-comment
- _post_id_           : the id for this specific item
- _retrieved_utc_     : when it was retrived by SingleFile adjusted to UTC
- _reactions_         : if `type == 'reactions'` this is a list of reactions (see below)

#### Comments
`comment.as_dict` returns the following:
-  _file_id_          : file in which this is found (matches page.ref_id) <_ZF3knE1jfpjpmljm_>
-  _date_              : date of post as found on FB when retrieved <_Tuesday at 3:27 PM_>
-  _date_utc_         : date of post normalized to UTC <_2020-07-07 17:27:48_>
-  _display_name_     : display name of the comment owner <_محمد عمر_>
-  _fb_id_            : Facebook id of the specific comment <_616111729289804_>
-  _parent_           : id of the object above (if it is the fb_id, it's a post) <_616103579290619_>
-  _reactions_        : number of reactions <_60_>
-  _sentiment_        : rough type of reactions reported in order from most to third-most <[_Like_, _Haha_, _Love_]>
-  _text_             : post texts <_رد سريع وقاسي الله يبارك_>
-  _top_level_post_id_: top level post <_616103579290619_>
-  _username_         : username for navigating facebook - could be number or string <_100001749670760_>
- _images_            : a list of dictionaries containing image information if existing
- _connections_       : a list of tags of users or hashtags (see below)
- _position_          : is it a post, comment, or subcomment?

##### Images
`comment.image[<index>]` returns the following
- _src_               : Image encoded as jpeg base64 (possibly also png, though right now I'm filtering those)
- _alt_               : Alt text - usually machine learning guess of contents of image - valuable
- _width_             : image width in px
- _height_            : image height in px
- _class_             : class attribute used by FB - probably unnecessary

##### connections
`comment.connections` contains hashtags or users lists
- _hashtags_
    -   _tag_url_     : url format of hashtag used in `mbasic.facebook.com/hashtag/<tag_url>`
    -   _tag_text_    : display text of hashtag (eg. #blacklivesmatter)
- _users_
    - _username_      : same as comment record  - connects users by tag and conversation interaction or association
    - _display_name_  : display name of user **could be different from other, more complete records** because you can use just first name (though most don't)
- _links_
    - _text_          : text of linked content
    - _link_          : link to linked content
