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
