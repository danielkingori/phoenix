"""Test the graph utilities."""
import mock

from phoenix.tag.graphing import graph_utilities


@mock.patch("tentaclio.open")
def test_save_graph_s3(m_open):
    """Test the save of graph with s3 url."""
    graph = mock.Mock()
    path = "s3://bucket/graph.html"
    graph_utilities.save_graph(graph, path)
    content_type = "text/html"
    m_open.assert_called_once_with(path, "w", upload_extra_args={"ContentType": content_type})
    m_open.return_value.__enter__().write.assert_called_once_with(graph.html)


@mock.patch("tentaclio.open")
def test_save_graph_file(m_open):
    """Test the save of graph with file url."""
    graph = mock.Mock()
    path = "file://bucket/graph.html"
    graph_utilities.save_graph(graph, path)
    m_open.assert_called_once_with(path, "w")
    m_open.return_value.__enter__().write.assert_called_once_with(graph.html)


@mock.patch("phoenix.tag.graphing.graph_utilities.save_graph")
def test_save_dashboard_graph_set(m_save_graph):
    """Test the save dashboard graph."""
    graph = mock.Mock()
    path = "url"
    graph_utilities.save_dashboard_graph(graph, path)
    m_save_graph.assert_called_once_with(graph, path)


@mock.patch("phoenix.tag.graphing.graph_utilities.save_graph")
def test_save_dashboard_graph_not_set(m_save_graph):
    """Test the save dashboard graph."""
    graph = mock.Mock()
    path = None
    graph_utilities.save_dashboard_graph(graph, path)
    m_save_graph.assert_not_called()
