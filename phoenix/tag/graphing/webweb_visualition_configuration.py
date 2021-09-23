"""Configurations with webweb for different graphs."""

import networkx as nx
from webweb import Web


def create_retweet_visualization(graph: nx.Graph):
    """Create network graph visualization."""
    web = Web(
        nx_G=graph,
    )
    web.title = "Twitter retweets graph"
    web.display.scaleLinkWidth = True
    web.display.scaleLinkOpacity = True
    web.display.charge = 40
    web.display.linkLength = 50
    web.display.linkStrength = 1
    web.display.gravity = 0.2
    # web.display.colorPalette = 'Dark2' # This is not working
    web.display.colorBy = "community"
    web.display.sizeBy = "degree"
    web.display.width = 1200
    web.display.height = 1200
    # web.show()
    return web


def create_facebook_topic_to_topic_visualization(graph: nx.Graph):
    """Create network graph visualization for Facebook topic-to-topic graph."""
    # Create graph
    web = Web(nx_G=graph)
    # Set settings
    web.title = "Facebook post topics"
    web.display.linkLength = 400
    web.display.scaleLinkWidth = True
    web.display.scaleLinkOpacity = True
    web.display.showNodeNames = True
    web.display.colorBy = "community"
    web.display.sizeBy = "degree"
    web.display.width = "1200"
    web.display.height = "1200"
    return web


def create_twitter_friends_visualization(graph: nx.Graph):
    """Create network graph visualization."""
    web = Web(nx_G=graph)
    web.display.scaleLinkWidth = True
    web.display.scaleLinkOpacity = True
    web.display.linkLength = 25
    web.display.linkStrength = 2
    web.display.charge = 10
    web.display.gravity = 0.2
    web.display.colorBy = "community"
    web.display.sizeBy = "degree"
    web.display.width = 1200
    web.display.height = 1200
    # web.show()
    return web
