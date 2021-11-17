"""Test search functionality."""
import mock
import pandas as pd

from phoenix.scrape.youtube import search_videos as search


YOUTUBE_MODULE_STR = "phoenix.scrape.youtube"


@mock.patch(f"{YOUTUBE_MODULE_STR}.lists.paginate_list_resource")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_part_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.datetime_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_resource_client")
def test_get_videos_for_channel_defaults(
    m_get_resource_client, m_datetime_str, m_get_part_str, m_paginate_list_resource
):
    """Test get_videos_for_channel with defaults."""
    channel_id = "channel_id"
    published_after = mock.Mock()
    result = search.get_videos_for_channel(channel_id, published_after)
    m_get_resource_client.assert_called_once_with(search.RESOURCE_CLIENT, None)
    resource_client = m_get_resource_client.return_value
    m_get_part_str.assert_called_once_with(search.DEFAULT_PARTS_TO_REQUEST)
    m_datetime_str.assert_called_once_with(published_after)
    resource_client.list.assert_called_once_with(
        part=m_get_part_str.return_value,
        channelId=channel_id,
        order=search.DEFAULT_ORDER,
        publishedAfter=m_datetime_str.return_value,
        type=search.TYPE_VIDEO,
        maxResults=50,
    )
    request = resource_client.list.return_value
    m_paginate_list_resource.assert_called_once_with(resource_client, request, max_pages=1)
    assert result == m_paginate_list_resource.return_value


@mock.patch(f"{YOUTUBE_MODULE_STR}.lists.paginate_list_resource")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_part_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.datetime_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_resource_client")
def test_get_videos_for_channel(
    m_get_resource_client, m_datetime_str, m_get_part_str, m_paginate_list_resource
):
    """Test get_videos_for_channel set params."""
    channel_id = "channel_id"
    published_after = mock.Mock()
    parts_list = mock.Mock()
    order = "order"
    client = mock.Mock()
    result = search.get_videos_for_channel(
        channel_id, published_after, parts_list=parts_list, order=order, client=client
    )
    m_get_resource_client.assert_called_once_with(search.RESOURCE_CLIENT, client)
    resource_client = m_get_resource_client.return_value
    m_get_part_str.assert_called_once_with(parts_list)
    m_datetime_str.assert_called_once_with(published_after)
    resource_client.list.assert_called_once_with(
        part=m_get_part_str.return_value,
        channelId=channel_id,
        order=order,
        publishedAfter=m_datetime_str.return_value,
        type=search.TYPE_VIDEO,
        maxResults=50,
    )
    request = resource_client.list.return_value
    m_paginate_list_resource.assert_called_once_with(resource_client, request, max_pages=1)
    assert result == m_paginate_list_resource.return_value


@mock.patch(f"{YOUTUBE_MODULE_STR}.search_videos.get_videos_for_channel")
def test_get_videos_for_channel_config(m_get_videos_for_channel):
    """Test get_videos_for_channel_config set params."""
    id_1 = "id_1"
    id_2 = "id_2"
    channel_config = pd.DataFrame({"channel_id": [id_1, id_2]})
    published_after = mock.Mock()
    result = search.get_videos_for_channel_config(channel_config, published_after)
    mock_result = m_get_videos_for_channel.return_value
    calls = [
        mock.call(id_1, published_after),
        mock.call().__radd__([]),
        mock.call(id_2, published_after),
        mock.call().__radd__([]).__add__(mock_result),
    ]
    m_get_videos_for_channel.assert_has_calls(calls)
    assert result == m_get_videos_for_channel().__radd__().__add__()
