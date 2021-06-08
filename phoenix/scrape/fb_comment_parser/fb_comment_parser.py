# type: ignore
"""Extract HTML elements from facbook pages.

Script      : Soup Parser
Created     : June, 2020
Updated     : June 2021
Author(s)   : Jacob Lefton and Chris Orwa
Version     : v1.2

"""

import json
import logging
import re
from urllib import parse

# laod required libraries
from bs4 import BeautifulSoup
from bs4 import Comment as htmlComment

# load pre-defined scripts
from phoenix.scrape.fb_comment_parser.date_parser import get_retrieved_date
from phoenix.scrape.fb_comment_parser.date_parser import main as date_parser_main
from phoenix.scrape.fb_comment_parser.date_parser import return_datestring


# TODO bring into line with mypy

# def generate_random_id():
#     return ''.join(random.choices(string.ascii_letters + string.digits, k=16))


def test_is_file_mbasic(file, file_name):  # noqa
    # Sometimes pages come in that aren't mbasic files - we can tell
    # because they're either too big, or the url: saved with SingleFile
    # is wrong
    length = 0
    while True:
        # Increase the search at the head of the file until a comment is found.
        # It will be SingleFile's comment.
        soup = BeautifulSoup(file[:length], "html.parser")
        comments = soup.find_all(string=lambda text: isinstance(text, htmlComment))
        if comments:
            break
        length += 500
    comment = comments[0].split()
    url = parse.urlsplit(comment[comment.index("url:") + 1])
    if url.netloc == "mbasic.facebook.com":
        return True
    else:
        print(f"File {file_name} is not a recognized mbasic resource.")


# create Page class
class Page(object):  # noqa

    # initialize page class
    def __init__(self, raw_file: str, file_name: str = None):

        if test_is_file_mbasic(raw_file, file_name):
            self.raw = raw_file
            self.setup()
            if self.test_page_url() and not self.content_not_found(file_name):
                self.create_page(file_name)
                self.parse_status = True
            else:
                self.parse_status = False
        else:
            self.parse_status = False

    def content_not_found(self, file_name):
        """Tests if file is a 'Content not found' page."""
        if "Content not found" in self.soup.title.text:
            print(f'File {file_name} is a "Content not found" mbasic page.')
            return True

    def test_page_url(self):
        """Tests if path is from a search page."""
        if "search" not in self.url_components["path_components"]:
            # sometimes path is /search/posts/
            return True
        else:
            print(f"Not valid parser path: {self.url_components}")

    def setup(self):
        """Set up core components of the soup and url components important for page validity."""
        self.soup: BeautifulSoup = BeautifulSoup(self.raw, "html.parser")
        logging.debug("BeautifulSoup created")
        # Extract and save URL
        self.url = self.get_page_url()
        self.url_components = self.get_url_components()
        logging.debug(f"url components: {self.url_components}")

    def create_page(self, file_name):
        """Intake raw file and convert to BeautifulSoup format."""
        # self.raw = raw_file
        # self.size = size(Path(name).stat().st_size)

        # Generate random id for the file record
        self.file_id = file_name

        # Extract and save metadata
        self.metadata_raw = self.get_raw_metadata()
        self.metadata = None
        # Set necessary variables
        self.page_type = None
        self.parent_post = None  # if top post, self, if react or reply, it's a post
        self.top_post_owner = (
            None  # the original post creator who instantiated this particular page
        )
        self.page_id = None  # what fb page it's on
        self.post_id = None  # the id of this post if not reactions
        self.retrieved_utc = None  # timestamp of the download
        self.reactions = []  # accurate reactions, from a reactions page
        self.comments = []

        logging.debug(f"len(metadata_raw): {len(self.metadata_raw)}")

        # Determine page type by analyzing url_components['path']
        # changes self.type, self.page_id, self.url_components['path_components'],
        self.analyze_path_for_page_type()
        logging.debug(f"type: {self.page_type}")

        if self.page_type == "reaction":
            self.back_link = self.get_back_link()

        # to set self.metadata and self.type if possible
        # self.derive_metadata()
        self.metadata = self.get_page_metadata()
        logging.debug(f"metadata: {self.metadata}")

        # relies on self.type being set. Returns None, and sets parent_post in function.
        self.get_parent_post()
        logging.debug(f"parent_post: {self.parent_post}")

        # get the retrieved date using specific date_parser methods
        self.retrieved_utc = return_datestring(get_retrieved_date(self.soup))
        logging.debug(f"retrieved_utc: {self.retrieved_utc}")

        # what is really the title of the page? This title is not always useful
        self.title = self.get_page_title()
        logging.debug(f"title:{self.title}")

        # build metadata
        if self.page_type in ["group post", "story", "photos"]:
            self.page_id = self.get_page_id()
            self.top_post_owner = self.get_top_post_owner()
            self.post_id = self.parent_post

        elif self.page_type == "replies":
            if "_ft_" in self.url_components["query"].keys():
                self.post_id = self.url_components["query"]["ctoken"][0].split("_")[1]
                self.page_id = self.url_components["query"]["_ft_"]["page_id"]
                self.top_post_owner = self.url_components["query"]["_ft_"]["content_owner_id_new"]

            elif "ctoken" in self.url_components["query"].keys():
                self.post_id = self.url_components["query"]["ctoken"][0].split("_")[1]
                self.page_id = self.get_page_id()
                self.top_post_owner = self.get_top_post_owner()

            # get replies subcomments
            self.get_subcomments()

        elif self.page_type == "reaction":
            self.page_id = self.get_reaction_page_id()
            self.top_post_owner = self.get_reaction_top_post_owner()
            self.post_id = self.parent_post
            self.reactions = self.get_accurate_reacts()

        logging.debug(f"page_id: {self.page_id}")
        logging.debug(f"top_post_owner: {self.top_post_owner}")
        logging.debug(f"post_id: {self.post_id}")
        logging.debug(f"reactions: {self.reactions}")

        # Comment time!
        if self.page_type == "photos":
            # The main post and comments will be below the image. We will get the lower comments
            self.photo_comments = []
            self.photo_comments = self.find_photo_comments()
            if self.photo_comments:
                self.parse_photo_comments()
            self.get_top_photo_post()

        else:
            self.comment_section = self.get_comment_section()
            self.get_comments()

        # it's weird to set this out here, but I didn't want to send a soup to
        # every Comment call, so I imported the date-parser utilities separately
        for comment in self.comments:
            comment.date_utc = date_parser_main(self.soup, comment.date)
            comment.as_dict["date_utc"] = comment.date_utc
            comment.file_id = self.file_id
            comment.as_dict["file_id"] = self.file_id

        # check if shared posts have other insights in their metadata - such as
        # content shared or something

        # if it's a photo post, it might not be able to find the top post owner,
        # so set it as the owner of the top comment if it's a post
        self.top_post_username = self.top_post_owner_username()

        self.build_dict()

        self.json = json.dumps(self.as_dict)

    # ===========================================
    # methods start here
    # ===========================================

    def build_dict(self):
        """Build dictionary from object attributes."""
        self.as_dict = {
            "page": {
                "file_id": self.file_id,
                "title": self.title,
                "page_type": self.page_type,
                "page_id": self.page_id,
                "top_post_owner": self.top_post_owner,
                "top_post_owner_username": self.top_post_username,
                "parent_post": self.parent_post,
                "post_id": self.post_id,
                "retrieved_utc": self.retrieved_utc,
                "reactions": self.reactions,
            },
            "comments": [],
        }
        for comment in self.comments:
            self.as_dict["comments"].append(comment.as_dict)

    def top_post_owner_username(self):
        """Get the top post owner username or page name."""
        # could do self.comments[-1], because right now top post is always
        # in the end, but if we change that then there's no hidden breaking
        name = None
        for comment in self.comments:
            if comment.position == "post":
                # only if the comment is a post! In replies this won't work, for sure.
                name = comment.as_dict["username"]
            else:
                pass
        return name

    def get_raw_metadata(self):
        """Get the raw metadata from the html."""
        return self.soup.find_all("div", attrs={"data-ft": True})

    def get_page_metadata(self: BeautifulSoup) -> dict:
        """Get HTML component containing required data."""
        #
        # Args:
        #   soup: a soup object from Beautiful Soup
        #
        # Returns:
        #   json interpreted soup.find(): soup string matched object
        # ===========================================================================

        # Parse the html file
        # html_page = codecs.open(self.name,"r","utf-8")
        # soup = BeautifulSoup(html_page, 'html.parser')
        try:
            return json.loads(self.soup.find_all("div", attrs={"data-ft": True})[0]["data-ft"])
        except Exception:
            # TODO create better checks for this exception
            logging.debug(
                'No "data-ft" attributes. likely reaction or replies with no images - '
                "should parse correctly"
            )
            return {}

    def get_page_url(self):
        """Get url of the page."""
        #
        # Args:
        #   soup: a soup object from Beautiful Soup
        #
        # Returns:
        #   soap.find(): soap string matched object
        return self.soup.find("link", attrs={"rel": "canonical"})["href"]

    def get_url_components(self, url=None):
        """Parses url, gets the query and parameters."""

        def ft_parse(params):
            # ===========================================================================
            # sub parsing function to deal with '_ft_' element
            # ===========================================================================
            ft = params["_ft_"][0].split(":")
            ft_dict = {}
            for x in ft:
                y = x.split(".")
                try:
                    ft_dict[y[0]] = y[1]
                except IndexError:
                    pass
            return ft_dict

        if url:
            # This is specifically to allow identification of other urls
            url_parse = parse.urlsplit(url)
        else:
            url_parse = parse.urlsplit(self.url)
        url_query = url_parse.query
        url_params = parse.parse_qs(url_query)
        path_components = url_parse.path.strip("/").split("/")

        if "_ft_" in url_params:
            url_params["_ft_"] = ft_parse(
                url_params
            )  # call sub-function to get dict of '_ft_' element

        url_components = {
            "scheme": url_parse.scheme,
            "netloc": url_parse.netloc,
            "path": url_parse.path,
            "path_components": path_components,
            "query": url_params,
        }
        return url_components

    def analyze_path_for_page_type(self):
        """Analyzes the page path and determines type of page."""
        p_c = self.url_components["path_components"]
        if "replies" in p_c:
            # --> '/comment/replies/'
            self.page_type = "replies"
        elif "story.php" in p_c:
            # --> /story.php
            self.page_type = "story"
        elif "reaction" in p_c:
            # --> '/ufi/reaction/profile/browser/'
            self.page_type = "reaction"
        elif "groups" in p_c:
            # --> '/groups/550843951711544'
            self.page_type = "group post"
        elif "posts" in p_c:
            # --> /molazemale/posts/616103579290619/
            self.page_type = "story"
        elif "photos" in p_c:
            # self.soup.find('div',
            #               attrs={'id': 'MPhotoActionbar'}).previous_sibling.img for image:
            # photo is positioned above the post
            self.page_type = "photos"
        elif not self.url_components["query"]:
            if len(p_c) <= 1:
                self.page_type = "wall"

        # self.url_components['path_components'] = p_c

    def get_parent_post(self):  # noqa: C901
        """Get parent post ('Top Level post ID')."""
        # TODO: Function is too complex

        query = self.url_components["query"]
        path = self.url_components["path_components"]
        if self.page_type == "story":
            if "story.php" not in path:
                # this has a reliable url where
                # path = ['watchthemed.alarmphone', 'posts', '2685185111755715']
                self.parent_post = path[-1]
            # elif 'story.php' in path:
            #     self.parent_post = query['_ft_']['top_level_post_id']
        if not self.parent_post:
            if "top_level_post_id" in self.metadata.keys():
                self.parent_post = self.metadata["top_level_post_id"]
            elif "_ft_" in query.keys():
                if "top_level_post_id" in query["_ft_"]:  # could be group post or reply
                    self.parent_post = query["_ft_"]["top_level_post_id"]
                elif "mf_story_key" in query["_ft_"]:  # this is not reliable
                    self.parent_post = self.url_components["query"]["_ft_"]["mf_story_key"]
            elif "ctoken" in self.url_components["query"].keys():
                logging.debug("c_token found")
                self.parent_post = self.url_components["query"]["ctoken"][0].split("_")[0]
                # also in ['query']['ft_ent_identifier'][0]
            elif self.page_type == "photos":
                # path --> mbasic.facebook.com/GovernorMikeSonkoMbuvi/
                #                 photos/a.187136547991368/840811325957217/<...>
                self.parent_post = self.url_components["path_components"][-1]
        if not self.parent_post:
            if self.page_type == "reaction":
                # Basically the url has nothing useful in it for us, and we are going
                # to use the back link
                self.parent_post = self.back_link["query"]["story_fbid"][0]

    def get_top_post_owner(self):
        """Gets owner of the top level post if available in metadata."""
        # Set the id strings for shorter lines
        new_id = "content_owner_id_new"
        old_id = "content_owner_id"
        tpo = None  # top_post_owner
        if "_ft_" in self.url_components["query"]:
            if new_id in self.url_components["query"]["_ft_"]:
                tpo = self.url_components["query"]["_ft_"]["content_owner_id_new"]
        elif self.metadata:
            if new_id in self.metadata.keys():
                tpo = self.metadata[new_id]
            if old_id in self.metadata.keys():
                tpo = self.metadata[old_id]
        elif type == "photos":
            tpo = None
        return tpo

    def get_page_id(self):  # noqa: C901
        """Gets page_id from metadata with allowance for further ids."""
        # TODO: function is too complex
        page_id = ""
        query = self.url_components["query"]
        try:
            if self.page_type == "group post":
                # --> '/groups/550843951711544'
                page_id = self.url_components["path_components"][-1]
            elif self.metadata:
                if "page_id" in self.metadata.keys():
                    # Check if it's a shared post from another page
                    if "original_content_owner_id" not in self.metadata.keys():
                        page_id = self.metadata["page_id"]
                elif "page_insights" in self.metadata.keys():
                    page_id = list(self.metadata["page_insights"].keys())[0]
                elif "_ft_" in query:
                    if "content_owner_id_new" in query["_ft_"].keys():
                        page_id = self.url_components["query"]["_ft_"]["content_owner_id_new"]
            # Some subcomment pages have metadata, so if still not page_id
            if not page_id:
                if "View post" in self.soup.header.a.text:
                    # It's a reply page - no page id easily visible
                    # found in View post back link -->
                    #       '/story.php?story_fbid=616103579290619&id=314842512750062'
                    parent_link = self.soup.header.a["href"]
                    link_features = parse.urlsplit(parent_link)
                    link_features_query = parse.parse_qs(link_features.query)
                    if "id" in link_features_query.keys():
                        page_id = ["id"][0]
                    elif link_features.fragment:
                        # This might be a back-link on a photo post with a weird url
                        #       --> fragment='comment_form_148950081810015_840811325957217'
                        for frag in link_features.fragment.split("_"):
                            try:
                                page_id = int(frag)
                                break  # because the first fragment is the page_id
                            except ValueError:
                                continue

        except AttributeError:
            logging.debug(
                "Attribute error in get_page_id(); "
                "page may be photo post without strong url query"
            )
            # If query is missing in page url, find first url from <div id="objects_container"...
            # which is the url in a different format in my test page (j-sonko.html)
            if self.page_type == "photos":
                page_id = self.photos_rescue_ids()
            else:
                page_id = self.metadata[0]["data-ft"]["page_id"]

        if self.page_type == "replies" and page_id == "id":
            logging.debug("Edge case with not easily picking up an ID in replies page.")
            container = self.soup.find("div", {"id": "objects_container"})
            url_components = self.get_url_components(url=container.a["href"])
            page_id = url_components["query"]["id"][0]
        return page_id

    def get_back_link(self):
        """Gets the 'back' link for a page if it exists."""
        for a in self.soup.find_all("a"):
            if "Back to post" in a.text:
                return self.get_url_components(url=a["href"])

    def get_reaction_page_id(self):
        """Gets the id of the fb page from a page of reactions."""
        # some reaction pages don't have a page_id in the url, so next place
        # to look is the 'back to post' button, which is a url back to the post.
        if hasattr(
            self.url_components["query"],
            "_ft_",
        ):
            page_id = self.url_components["query"]["_ft_"]["page_id"]
        else:
            page_id = self.back_link["query"]["id"][0]
            if not self.back_link:
                print("No back link found in this reactions page")
                raise
        return page_id

    def get_reaction_top_post_owner(self):
        """Get the page name for a reactions page."""
        if hasattr(
            self.url_components["query"],
            "_ft_",
        ):
            top_post_owner = self.url_components["query"]["_ft_"]["content_owner_id_new"]
        else:
            top_post_owner = self.back_link["query"]["id"][0]
            if not self.back_link:
                print("No back link found in this reactions page")
                raise
        return top_post_owner

    def photos_rescue_ids(self):
        """Get the id from a photos page."""
        url = self.soup.find("div", attrs={"id": "objects_container"}).a["href"]
        url_components = self.get_url_components(url)
        return url_components["query"]["id"][0]

    def get_page_title(self):
        """Get title of a HTML page."""
        #
        # Args:
        #   soup: a soup object from Beautiful Soup
        #
        # Returns:
        #   soup.header.h3: soup object
        # ===========================================================================
        # I would actually call this 'posting information' - who posted where
        # note: soup.head.title.text seems obvious but
        # - on an individual page, the title is entire post text
        # - on a group page, the title seems to be the name of the group
        # if this is a person posting in a group it will look like "person > group"
        # Parse the html file
        try:
            return self.soup.header.h3.text.strip("\n")
        except AttributeError:
            return None

    def get_accurate_reacts(self):
        """Find the specific react markers on a reaction page."""
        # number of reacts in <span> within <a aria-pressed=
        # type of reacts in <img alt=
        arias = []
        reacts = {}
        for a in self.soup.find_all("a"):
            if "aria-pressed" in a.attrs:
                arias.append(a)
        for a in arias:
            if a.span:
                reacts[a.img["alt"]] = self.parse_react_int(a.span.text)
        return reacts

    @staticmethod
    def parse_react_int(number):
        """Get the number of reactions from a weird string."""
        # These come in as strings, sometimes with extra junk on them.
        # Sometimes they look like: {'Love': '1.1K'}
        # solution from:
        # https://stackoverflow.com/questions/
        # 59507827/converting-a-string-object-value-with-m-and-k-to-million-and-thousand
        number = number.strip()
        if number[-1:] == "K":  # Check if the last digit is K
            converted_number = int(
                float(number[:-1]) * 1000
            )  # Remove the last digit with [:-1], and convert to int and multiply by 1000
        else:
            # it's just a number
            converted_number = int(number)
        return converted_number

    def get_comment_section(self, post_metadata={}):
        """Get comments section for a page."""
        # Args:
        #   html_path: a HTML document
        #   post_metadata: HTML tag uniquely identifying comment section
        #
        # Returns:
        #   soap.find(): soap string matched object
        # ===========================================================================
        # the bottom section seems to be under this ufi_[top-level-post-id]
        # <div class="e cj ck" id="ufi_2860539280741988"> <!-- below post content -->
        #
        # # Parse the html file
        # soup = BeautifulSoup(html_page, 'html.parser')
        return self.soup.find(attrs={"id": re.compile(r"ufi_[0-9]+")})

    def get_comments_from_comment_section(self):
        """Get section of HTML with section of embedded section."""
        # Args:
        #   soup: a soup object from Beautiful Soup
        #   post_metadata: HTML tag uniquely identifying comment section
        #
        # Returns:
        #   soap.find(): soap string matched object
        # ===========================================================================
        # <div class="e cj ck" id="ufi_2860539280741988"> <!-- below post content -->
        #  <div class="cl cm">
        #   <div class="dm cl"> <!-- comments -->
        #    <div class="dn" id="2860580207404562"> <!-- comment -->
        return self.comment_section.find_all("div", attrs={"id": re.compile(r"^[0-9]+")})

    def get_comment_section_parent(self):
        """Get id of the parent for a comment section."""
        return re.findall(r"[0-9]+", self.comment_section["id"])[0]

    def get_comments(self):
        """Get display & user names, text, top level id, and id for comment sections."""
        # Returns:
        #   display_name:
        #   user_name:
        #   text:
        #   top_level_post_id
        #   id: integer
        self.comment_section = self.get_comment_section()
        comment_parent = []
        comments_list = []
        try:
            comment_parent = self.get_comment_section_parent()
        except TypeError:
            # should know type of page here instead of try/except?
            # see metadata and other markers
            pass
        if self.comment_section:
            comments_list = self.get_comments_from_comment_section()

        if comments_list:
            for comment in comments_list:
                # get all comments underneath
                if not Comment.evaluate_as_reply_preview(comment):
                    # On a very rare occasion, the comment is going to be a preview of a
                    # reply thread and it has some distinct markers, but can't be read as
                    # a full comment object so we need to skip it here to make sure a
                    # ghost record doesn't enter the system
                    self.comments.append(
                        Comment(
                            comment=comment,
                            comment_parent=comment_parent,
                            top_level_post_id=self.parent_post,
                            page_type=self.page_type,
                        )
                    )
                # get top level post
                # pass metadata-raw and type
            self.comments.append(
                Comment(
                    comment=self.metadata_raw,
                    comment_parent=comment_parent,
                    top_level_post_id=self.parent_post,  # self.metadata['top_level_post_id'],
                    top_post=True,
                    soup=self.soup,
                    page_type=self.page_type,
                )
            )
        # This is weird - just handling the fact that I put the top-post under an
        #   if statement but when a post has no comments, it gets lost.
        if not comments_list:
            if self.page_type == "story" or self.page_type == "group post":
                self.comments.append(
                    Comment(
                        comment=self.metadata_raw,
                        comment_parent=comment_parent,
                        top_level_post_id=self.parent_post,  # self.metadata['top_level_post_id'],
                        top_post=True,
                        soup=self.soup,
                        page_type=self.page_type,
                    )
                )

    def get_subcomments(self):
        """Isolate the comments sections for subcomments and send to Comment object creator."""
        comment_list = self.soup.find_all(attrs={"id": re.compile(r"^[0-9]+")})
        for comment in comment_list:
            self.comments.append(
                Comment(
                    comment=comment,
                    comment_parent=self.post_id,
                    top_level_post_id=self.parent_post,
                    page_type=self.page_type,
                )
            )
        # Of course, the first comment on the page is the top comment but not a post,
        # so set its parent == top_level_post_id
        self.comments[0].parent = self.parent_post
        self.comments[0].as_dict["parent"] = self.parent_post

    def find_photo_comments(self):
        """Find comments in a photo post."""
        # ===========================================================================
        # Different from ufi_###, this scheme seems to have
        # ===========================================================================
        return self.soup.find_all(attrs={"id": re.compile(r"^[0-9]+")})

    def parse_photo_comments(self):
        """Get Comment object for each comment in the photo post."""
        # This is probably the same as the other comment sections so need to
        # restructure get_comments to allow for this
        for comment in self.photo_comments:
            if not Comment.evaluate_as_reply_preview(comment):
                self.comments.append(
                    Comment(
                        comment=comment,
                        comment_parent=self.post_id,
                        top_level_post_id=self.parent_post,
                        page_type=self.page_type,
                    )
                )

    def get_top_photo_post(self):
        """Get top-level comment for the photo post."""
        comment = self.soup.find("div", attrs={"id": "MPhotoContent"})
        image = self.soup.find("div", attrs={"id": "MPhotoActionbar"}).previous_sibling.img
        self.comments.append(
            Comment(
                comment=comment,
                image=image,
                comment_parent=self.post_id,
                top_level_post_id=self.parent_post,
                top_post=True,
                page_type=self.page_type,
            )
        )


class Comment(object):
    """Compile the comments from a page."""

    def __init__(
        self,
        comment=None,
        comment_parent=None,
        top_level_post_id=None,
        top_post=False,
        soup=None,
        page_type=None,
        image=None,
    ):

        if comment:
            self.raw = comment

        self.top_level_post_id = top_level_post_id
        self.parent = comment_parent

        self.date = ""
        self.date_utc = ""  # This gets set elsewhere in the Page object right now
        self.reactions = ""
        self.sentiment = []
        self.images = []
        self.connections = []
        self.position = ""

        # very occasionally facebook will preview a reply in
        # the comments and it will mess up the parser
        # if this happens, we don't want the comment.
        # self.reply_preview = self.evaluate_as_reply_preview()

        if top_post is True and page_type == "photos":
            # special exception if it's a photo post
            self.fb_id = comment_parent
            # get self.text, self.display_name, self.username
            self.get_photo_post_text_and_owner(comment)
            self.date = self.get_comment_date(comment)
            react_string = self.get_top_post_reactions(comment)
            self.images = self.get_photo_post_image(image)
            # self.connections = self.get_connections(comment, top_post)

        else:
            if top_post is False:  # and self.reply_preview == False:
                self.fb_id = self.get_comment_id(comment)
                self.text = self.get_comment_text(comment)
                self.display_name, self.username = self.get_comment_owner(comment)
                self.display_name = self.display_name.strip("\n")
                self.date = self.get_comment_date(comment)
                react_string = self.get_reported_reactions(comment)
                self.images = self.get_comment_images(comment)
                self.connections = self.get_connections(comment, top_post)

            elif top_post is True:  # top post
                logging.debug("top post")
                self.text = comment[1].text
                self.fb_id = comment_parent
                # Top post parent is same as fb_id, because we have page object attached
                self.display_name = self.get_top_post_display_name(comment)
                self.username = self.get_top_post_username(comment)
                self.date = self.get_comment_date(
                    comment[0]
                )  # comment will be is a result set list
                react_string = self.get_top_post_reactions(soup)
                self.images = self.get_comment_images(comment[0])
                self.connections = self.get_connections(comment, top_post)

        if react_string:
            self.reactions, self.sentiment = self.clean_reactions(react_string)
            # Sentiment order is important - highest, second highest, third highest

        self.position = self.analyze_position(page_type)

        self.as_dict = {
            "fb_id": self.fb_id,
            "top_level_post_id": self.top_level_post_id,
            "parent": self.parent,
            "display_name": self.display_name,
            "username": self.username,
            "text": self.text,
            "date": self.date,
            "date_utc": self.date_utc,
            "reactions": self.reactions,
            "sentiment": self.sentiment,
            "images": self.images,
            "connections": self.connections,
            "position": self.position,
        }

    @staticmethod
    def evaluate_as_reply_preview(comment):
        """Deal with a reply preview exception."""
        # this is an annoying little exception. Sometimes (very seldom) facebook offers a
        # reply preview mid-stream in the conversation. these reply previews do not have all
        # the features of a comment, and they do have one extra element: a profile picture
        flag = False
        if comment.img:
            if "alt" in comment.img.attrs:
                if "profile picture" in comment.img["alt"]:
                    print("Comment reply preview found.")
                    flag = True
        return flag

    def analyze_position(self, page_type):
        """Evaluates what position in a post / comment / subcomment thread."""
        if page_type == "replies":
            if self.fb_id == self.parent:
                position = "comment"
            elif self.fb_id != self.parent:
                position = "subcomment"
        elif self.fb_id == self.top_level_post_id:
            position = "post"
        else:
            position = "comment"
        return position

    @staticmethod
    def get_top_post_display_name(metadata_raw):
        """Get the text from the header element which contains the poster name."""
        return metadata_raw[0].header.h3.a.text

    @staticmethod
    def get_top_post_username(metadata_raw):
        """Get the url from the poster name in the header element, which is a link."""
        url_components = parse.urlsplit(metadata_raw[0].header.h3.a["href"])
        paths = url_components._asdict()["path"].split("/")
        for c in paths:  # for component in paths, it will be some form of ['', 'screen.name', '']
            if c:
                return c

    @staticmethod
    def get_comment_owner(comment):
        """Get username of commenter."""
        # Args:
        #   comment: comment id number
        #
        # Returns:
        #   display_name: string of commenter's name
        #   user_name: string with commenter username
        display_name = comment.h3.text
        url_components = comment.h3.a["href"].split("/")

        if (
            url_components[-1][0] == "?"
        ):  # 'https://mbasic.facebook.com/richardamdenganvideoeditor/?refid=18&__tn__=R'
            username = url_components[-2]
        else:  # 'https://mbasic.facebook.com/erkumag?refid=18&__tn__=R'
            username = url_components[-1].split("?")[0]

        if (
            username == "profile.php"
        ):  # https://mbasic.facebook.com/profile.php?id=100001749670760&amp;rc=p&amp;__tn__=R
            username = extract_profile_id_from_link(comment.h3.a["href"])

        return display_name, username

    @staticmethod
    def get_comment_text(comment):
        """Get text of the comment."""
        # Args:
        #   comment: string of containing the comment
        #
        # Returns:
        #   comment.h3.next_sibling.text: HTML tag with comment text
        try:
            return comment.h3.next_sibling.text
        except AttributeError:
            return comment.h3.next_sibling.next_sibling.text

    def get_photo_post_text_and_owner(self, comment):
        """Get text and owner of photo post."""
        text_element = comment.div.div
        # get display name
        self.display_name = text_element.strong.text
        # get username from link
        # !! This might need debugging because it only accounts for
        # "path='/GovernorMikeSonkoMbuvi/' construction"
        user_link = text_element.a["href"]
        self.username = parse.urlsplit(user_link).path.strip("/")
        self.text = text_element.div.text

    @staticmethod
    def get_photo_post_image(image):
        """Get image from photo post."""
        return [image.attrs]

    @staticmethod
    def get_comment_id(comment):
        """Get comment id."""
        return comment["id"]

    @staticmethod
    def get_comment_date(comment):
        """Get date of comment."""
        return comment.abbr.text

    @staticmethod
    def get_reported_reactions(comment):
        """Get reported reactions --> ex. '60 reactions, including Like, Haha and Love'."""
        # Reported reactions are always in an 'aria-label', as are a few other things
        aria = ""
        for elem in comment.find_all("a"):
            if "aria-label" in elem.attrs:
                if "Watch" not in elem["aria-label"].split():
                    aria = elem["aria-label"]
        return aria

    @staticmethod
    def get_top_post_reactions(soup):
        """Get reactions of the top post."""
        # top posts come in the Beautiful Soup object, not in the metadata
        # as currently constructed
        aria = ""
        for elem in soup.find_all("div"):
            if "aria-label" in elem.attrs:
                aria = elem["aria-label"]
        return aria

    @staticmethod
    def clean_reactions(react_string):
        """Turn '20K left reactions including Like, Love and Care' into organized data."""
        # alternative format 'Lowai Hazam and 13K others left reactions,
        # including Like, Love and Care' with arabic can be like this:
        # '\u200e\u200eاسامة شلوف\u200e and 9.6K others\u200e...
        react_string = react_string.replace(",", "").replace("\u200e", "")
        react_elem = react_string.split()
        for elem in react_elem:
            if elem == "and":
                react_elem.pop(react_elem.index("and"))
        if "others" in react_elem:
            reactions = Page.parse_react_int(react_elem[react_elem.index("others") - 1])
            # pass the reactions to the Page function that interprets '20k' as 20000
        else:
            reactions = Page.parse_react_int(react_elem[0])

        # Now it should be: 20K left reactions including Like Love Care'
        sentiment = []
        for s in reversed(react_elem):
            if s == "including":
                break
            sentiment.append(s)
        # sentiment is constructed backward, so reverse
        return reactions, list(reversed(sentiment))

    @staticmethod
    def get_comment_images(comment):
        """Get images from comment in <img> tags."""
        imgs = comment.find_all("img")
        images = []
        if imgs:
            for img in imgs:
                # need to check if it's a small png
                # src='data:image/png;base64,<data....>'
                src = img["src"]
                src_slice = src.split(",")[0].split("/")[1].split(";")[0]
                if src_slice == "png" and "height" in img.attrs:
                    if img["height"] == 14:
                        pass
                else:
                    images.append(
                        img.attrs
                    )  # , img) # --> can append full <img> tag for direct web display
        return images

    def get_connections(self, comment, top_post=False):  # noqa: C901
        """Get connections - hashtags, links, users - from a comment."""
        # TODO: function is too complex
        # How to get tagged users in comment texts -
        #   very similar to get comment text but then look for links
        #   Also, hashtags can be captured in this way
        try:
            # first search in the comment needs to look under the h3,
            # because h3 has link to post owner
            links = comment.h3.next_sibling.find_all("a")
        except AttributeError:  # Same as get_comment_text -> it's sometimes a second sibling
            try:
                links = comment.h3.next_sibling.next_sibling.find_all("a")
            except AttributeError:  # It could be a top-post
                pass
        if top_post is True:  # It's a top-post element and doesn't have h3 pieces
            links = []
            for comm in comment:  # comment is a list
                links.extend(comm.find_all("a"))

        connections = {
            "hashtags": [],
            "users": [],
            "links": [],
        }

        for link in links:
            href_parse = parse.urlsplit(link["href"])
            path = href_parse.path
            flag = None

            # Need to get rid of various facebook links because we're grabbing
            # everything on the page - better to make exceptions than try to
            # find the specific terminology for every link
            exceptions = ["/edits/", "/save/", "/nfx/", "/video_redirect/", "/a/"]
            for exc in exceptions:
                if exc in path:
                    flag = True
            if flag:
                continue

            # Check standard link formats
            if href_parse.netloc == "mbasic.facebook.com":
                path_components = href_parse.path.split("/")

                # capture hashtags in the path '/hashtag/<tag>
                if "hashtag" in path_components:
                    ht = path_components[-1]
                    connections["hashtags"].append(
                        {
                            "hashtag_url": ht,
                            "hashtag_text": link.text.replace("\n", ""),
                        }
                    )
                else:
                    # Display name is what we read, always in the same place
                    display_name = link.text

                    if "story.php" in path_components:
                        # need to find examples for this type of connection
                        pass

                    elif "profile.php" in path_components:
                        # This is a user
                        username = extract_profile_id_from_link(link["href"])
                    else:
                        username = path_components[-1]  # capture users in the path '/<username>

                    if username == self.username or display_name == self.display_name:
                        # Make sure
                        continue
                    elif not display_name or not username:
                        continue

                    conn = {
                        "username": username,
                        "display_name": display_name,
                    }
                    if conn not in connections["users"]:
                        connections["users"].append(conn)

            elif href_parse.netloc == "lm.facebook.com":
                query = parse.parse_qs(href_parse.query)
                for key in query:
                    if "http://" in query[key][0] or "https://" in query[key][0]:

                        conn = {
                            "text": link.text.replace("\n", ""),
                            "link": query[key][0].split("fbclid")[0],
                        }  # get rid of fb tracking links
                        if conn not in connections["links"]:
                            connections["links"].append(conn)
                        break
        return connections


def extract_profile_id_from_link(url):
    """Handles exception link: https://mbasic.facebook.com/profile.php?id=100035504860378 ."""
    #   user has likely not set a username
    # seems weird to put it outside of the class, but I couldn't call it within the class
    #   because I made everythings static methods...
    try:
        query = parse.parse_qs(parse.urlsplit(url).query)
    except TypeError:
        print(url)
    return query["id"][0]
