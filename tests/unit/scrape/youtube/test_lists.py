"""Test Utils."""
import mock

from phoenix.scrape.youtube import lists


def test_default_list_process_function():
    """Test default list process function."""
    result_in = [1, 2]
    found_resources = 3
    assert lists.default_list_process_function(result_in, found_resources) == [1, 2, 3]


@mock.patch("phoenix.scrape.youtube.lists.default_list_process_function")
def test_paginate_list_resource_default(m_default_list_process_function):
    """Test paginate_list_resource."""
    resource_client = mock.Mock()
    list_next_fn = resource_client.list_next
    list_next_fn.return_value = None
    request = mock.Mock()
    execute_fn = request.execute
    result = lists.paginate_list_resource(resource_client, request)
    assert result == m_default_list_process_function.return_value
    execute_fn.assert_called_once_with()
    m_default_list_process_function.assert_called_once_with([], execute_fn.return_value)
    resource_client.list_next.assert_called_once_with(request, execute_fn.return_value)


@mock.patch("phoenix.scrape.youtube.lists.default_list_process_function")
def test_paginate_list_resource_with_result(m_default_list_process_function):
    """Test paginate_list_resource."""
    resource_client = mock.Mock()
    list_next_fn = resource_client.list_next
    list_next_fn.return_value = None
    request = mock.Mock()
    execute_fn = request.execute
    init_result = mock.Mock()
    result = lists.paginate_list_resource(resource_client, request, None, init_result)
    assert result == m_default_list_process_function.return_value
    execute_fn.assert_called_once_with()
    m_default_list_process_function.assert_called_once_with(init_result, execute_fn.return_value)
    resource_client.list_next.assert_called_once_with(request, execute_fn.return_value)
