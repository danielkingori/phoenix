import os
from pprint import pprint
from datetime import datetime
from bs4 import BeautifulSoup
import logging

from soup_parser import Page

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

'''
get_files(path) - gets all raw files in the provided directory
get_single_file(path, document) - send a directory and file name and it returns the raw file
test_run(file) - provide a filename in the test directory and it returns a page object
batch_test() - runs a test on all files in a directory
save_pretty(soup, filename) - saves a prettified version of the html with the given filename for debuging purposes
'''

# 2021-04-26 TEST_FILE_PATH set to the "pretty_analysis_pages" because I didn't upload the other files I have
TEST_FILE_PATH = os.path.join(os.path.dirname(os.path.realpath('__file__')),
                              "phoenix", "fb_comment_parser", "test_pages", "sample_pages")
PRETTY_FILE_PATH = os.path.join(os.path.dirname(os.path.realpath('__file__')),
                                "phoenix", "fb_comment_parser", "test_pages", "pretty_analysis_pages")

def get_files(path:str=None):
    # Args: Directory path or None for test directory
    # Returns: raw html from the file
    if not path:
        path = TEST_FILE_PATH
    for file in os.listdir(path):
        print(f'Getting {file}...')
        yield get_single_file(path, file)

def get_single_file(path:str, document:str) -> str:
    # Sometimes you want the raw html of a file to
    with open(os.path.join(str(path),str(document)), 'rb') as f:
        html_raw = f.read()
    return html_raw

def test_run(file:str):
    # Args: filename of a file in the test file path
    # Returns: fully parsed page object
    logging.info(f'Checking {file}')
    html_file: str = get_single_file(TEST_FILE_PATH, file)
    page_obj: Page = Page(html_file)
    return page_obj

def batch_test():
    # Returns: generator for all files in directory, parsed
    for raw_html in get_files(): # test_file_path()):
        try:
            print(f'file contents: {raw_html[:100]}')
            page = Page(raw_html)
            yield page
        except Exception as e:
            time = datetime.now()
            timestr = time.strftime('%Y%m%d_%H-%M-%S')
            soup = BeautifulSoup(raw_html, 'html.parser')
            save_pretty(soup, f'failed_{timestr}.html')
            print(f'Failed file "failed_{timestr}.html"')
            pass

def save_pretty(soup, filename):
    # Save a beautiful-soup pretty version of the facebook page
    # Args: beautiful-soup object, desired filename
    with open(os.path.join(PRETTY_FILE_PATH, filename), 'w', encoding='utf-8') as f:
        f.write(soup.prettify())


if __name__ == "__main__":

    pages = []
    for page in batch_test():
        pages.append(page)

        if len(pages) > 2:
        	break

