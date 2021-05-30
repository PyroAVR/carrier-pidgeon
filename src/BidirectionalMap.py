from collections import defaultdict
# TODO: removing a node entirely. probably should include an option to also
# remove all links pointing to it... expensive.


class BidirectionalMap:
    """
    Two-way mapping of nodes. Think of it like a directed bipartite graph.
    There are two sets of isolated nodes with connections only to the other
    set. Connections are directional, but bidirectional mappings without an
    intermediate node are allowed. Additionally, each node in a set may map
    to any number of nodes in the opposite set.
    """
    def __init__(self):
        # using sets prevents duplicate links
        self.set_a = defaultdict(set)
        self.set_b = defaultdict(set)


    @property
    def a(self):
        """
        Known node names in A.
        """
        return self.set_a.keys()


    @property
    def b(self):
        """
        Known node names in B.
        """
        return self.set_b.keys()


    def create_island_a(self, a):
        """
        Register a node, but do not connect it to anything.
        Note that if this node is found in an unlink/disconnect call where
        purge=True, it will be removed.
        """
        self.set_a[a] = set()


    def create_island_b(self, b):
        """
        Register a node, but do not connect it to anything.
        Note that if this node is found in an unlink/disconnect call where
        purge=True, it will be removed.
        """
        self.set_b[b] = set()


    def neighbors_of(self, x):
        """
        Return all nodes connected to the given node, in set A and B.
        returns: (neighbors of x in A, neighbors of x in B). Either may be None
        if x is not found in that set. If x is found, but has no neighbors, an
        empty set is returned instead.
        """
        # we don't want to create a new set while doing a query, so we test for
        # existence here first.
        neighbors_a = None
        neighbors_b = None
        if x in self.set_a:
            neighbors_a = self.set_a[x]

        if x in self.set_b:
            neighbors_b = self.set_b[x]

        return neighbors_a, neighbors_b


    def link(self, a, b):
        """
        Connect a pair of nodes in the map, adding them if necessary. (two way)
        """
        node_a_conns = self.set_a[a]
        node_b_conns = self.set_b[b]
        node_a_conns.add(b)
        node_b_conns.add(a)


    def unlink(self, a, b, purge=False):
        """
        Disconnect two nodes from each other. Remove if no connections remain
        after disconnecting if purge is True. (two way)
        """
        node_a_conns = self.set_a[a]
        node_b_conns = self.set_b[b]
        try:
            node_a_conns.remove(b)

        except KeyError:
            ... # swallow exceptions

        try:
            node_b_conns.remove(a)

        except KeyError:
            ...

        if purge:
            # Remove references to allow memory to be re-used.
            if len(node_a_conns) == 0:
                del self.set_a[a]

            if len(node_b_conns) == 0:
                del self.set_b[b]


    def connect_ab(self, a, b):
        """
        Create a one-way link from a node in set A to a node in set B.
        """
        node_a_conns = self.set_a[a]
        node_a_conns.add(b)


    def connect_ba(self, b, a):
        """
        Create a one-way link from a node in set B to a node in set A.
        """
        node_b_conns = self.set_b[b]
        node_b_conns.add(a)


    def disconnect_ab(self, a, b, purge=False):
        """
        Remove the link from a node in set A to a node in set B. Remove if no
        connections remain after disconnecting if purge is True.
        """
        node_a_conns = self.set_a[a]
        try:
            node_a_conns.remove(b)
        except KeyError:
            ...

        if purge:
            if len(node_a_conns) == 0:
                del self.set_a[a]


    def disconnect_ba(self, b, a, purge=False):
        """
        Remove the link from a node in set B to a node in set A.
        """
        node_b_conns = self.set_b[b]
        try:
            node_b_conns.remove(a)
        except KeyError:
            ...

        if purge:
            if len(node_a_conns) == 0:
                del self.set_a[a]

