"""Utils for youtube."""
from typing import Any, List, Optional, Protocol


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
    process_function: Optional[ListProcessFunction] = None,
    result: Optional[ListResults] = None,
) -> ListResults:
    """Paginate through a list of resources.

    General functionality based on:
    https://github.com/googleapis/google-api-python-client/blob/main/docs/pagination.md
    Arguments:
        resource_client: Resource client such as `youtube.channels()`
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
    while request is not None:
        found_resource = request.execute()
        result = process_function(result, found_resource)
        request = resource_client.list_next(request, found_resource)
    return result
