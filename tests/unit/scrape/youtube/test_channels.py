"""Test channels functionality."""
import mock
import pandas as pd

from phoenix.scrape.youtube import channels


YOUTUBE_MODULE_STR = "phoenix.scrape.youtube"


def test_get_channel_ids_str():
    """Test _get_channel_ids_str."""
    df = pd.DataFrame({"channel_id": ["1", "2", "3"]})
    assert "1,2,3" == channels._get_channel_ids_str(df)


@mock.patch(f"{YOUTUBE_MODULE_STR}.lists.paginate_list_resource")
@mock.patch(f"{YOUTUBE_MODULE_STR}.channels._get_channel_ids_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_part_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_resource_client")
def test_get_channels_defaults(
    m_get_resource_client, m_get_part_str, m_get_channel_ids_str, m_paginate_list_resource
):
    """Test get_channels with Defaults."""
    channel_config = mock.Mock()
    result = channels.get_channels(channel_config)
    m_get_resource_client.assert_called_once_with(channels.RESOURCE_CLIENT, None)
    resource_client = m_get_resource_client.return_value
    m_get_channel_ids_str.assert_called_once_with(channel_config)
    m_get_part_str.assert_called_once_with(channels.DEFAULT_PARTS_TO_REQUEST)
    resource_client.list.assert_called_once_with(
        part=m_get_part_str.return_value, id=m_get_channel_ids_str.return_value, maxResults=50
    )
    request = resource_client.list.return_value
    m_paginate_list_resource.assert_called_once_with(resource_client, request, max_pages=10000)
    assert result == m_paginate_list_resource.return_value


@mock.patch(f"{YOUTUBE_MODULE_STR}.lists.paginate_list_resource")
@mock.patch(f"{YOUTUBE_MODULE_STR}.channels._get_channel_ids_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_part_str")
@mock.patch(f"{YOUTUBE_MODULE_STR}.utils.get_resource_client")
def test_get_channels(
    m_get_resource_client, m_get_part_str, m_get_channel_ids_str, m_paginate_list_resource
):
    """Test get_channels."""
    channel_config = mock.Mock()
    parts_list = mock.Mock()
    client = mock.Mock()
    result = channels.get_channels(channel_config, parts_list=parts_list, client=client)
    m_get_resource_client.assert_called_once_with(channels.RESOURCE_CLIENT, client)
    resource_client = m_get_resource_client.return_value
    m_get_channel_ids_str.assert_called_once_with(channel_config)
    m_get_part_str.assert_called_once_with(parts_list)
    resource_client.list.assert_called_once_with(
        part=m_get_part_str.return_value,
        id=m_get_channel_ids_str.return_value,
        maxResults=50,
    )
    request = resource_client.list.return_value
    m_paginate_list_resource.assert_called_once_with(resource_client, request, max_pages=10000)
    assert result == m_paginate_list_resource.return_value
