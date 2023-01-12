"""Utils for youtube."""
from typing import Any, List, Optional, Protocol

import logging

from googleapiclient import errors


ListResults = List[Any]


class ListProcessFunction(Protocol):
    """List Process function."""

    def __call__(self, result: ListResults, found_resources: Any) -> ListResults:
        """List process function."""
        ...


def default_list_process_function(result: ListResults, found_resources: Any) -> ListResults:
    """Append the found resources to the result."""
    return result + [found_resources]


def paginate_list_resource(
    resource_client,
    request,
    max_pages: int = 1,
    process_function: Optional[ListProcessFunction] = None,
    result: Optional[ListResults] = None,
) -> ListResults:
    """Paginate through a list of resources.

    General functionality based on:
    https://github.com/googleapis/google-api-python-client/blob/main/docs/pagination.md

    This functionality can be used for any resource client that has `list` and `list_next`.
    Lot's of clients have this but not all.

    Arguments:
        resource_client: Resource client such as `youtube.channels()`
        request: The request object, such as `youtube.channels().list(...)`
        max_pages (int): The maximum number of pages to request, each page being a separate API
            call and thus each page using another multiple of the API's quota cost.
        process_function (ListProcessFunction): Function that will take the found resources
            and process them. Default default_list_process_function.
        result (ListResults): An optional results that should be appended to to.

    Returns:
        ListResults with the found and processed resources.
    """
    if not result:
        result = []
    # Using if over default params so that the typing works
    if not process_function:
        process_function = default_list_process_function

    page_counter = 0
    while request is not None and page_counter < max_pages:
        try:
            page_counter += 1
            found_resource = request.execute()
            result = process_function(result, found_resource)
            request = resource_client.list_next(request, found_resource)
        except errors.HttpError as e:
            # We are catching the errors due to getting 403 HttpErrors
            # due to this issue:
            # https://stackoverflow.com/questions/70013112/commentthreads-with-chennel-id-parameter-returns-403-error-that-refers-to-video
            logging.info("Error with youtube request. Returning collected data.")
            logging.error(e)

    return result
