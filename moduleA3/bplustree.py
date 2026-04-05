"""
bplustree.py — B+ Tree Implementation for CallHub Phone Directory
CS 432 Databases | Module A

Follows the exact boilerplate structure provided by the instructor.
Node structure: BPlusTreeNode(order, is_leaf)
Tree structure: BPlusTree(order=8)
"""

from graphviz import Digraph


class BPlusTreeNode:
    def __init__(self, order, is_leaf=True):
        self.order    = order       # Maximum number of children a node can have
        self.is_leaf  = is_leaf     # Flag to check if node is a leaf
        self.keys     = []          # List of keys in the node
        self.values   = []          # Used in leaf nodes to store associated values
        self.children = []          # Used in internal nodes to store child pointers
        self.next     = None        # Points to next leaf node for range queries

    def is_full(self):
        # A node is full if it has reached the maximum number of keys (order - 1)
        return len(self.keys) >= self.order - 1


class BPlusTree:
    def __init__(self, order=8):
        self.order = order                       # Maximum number of children per internal node
        self.root  = BPlusTreeNode(order)        # Start with an empty leaf node as root

    # ------------------------------------------------------------------ #
    #  Search                                                              #
    # ------------------------------------------------------------------ #

    def search(self, key):
        """Search for a key in the B+ tree and return the associated value."""
        return self._search(self.root, key)

    def _search(self, node, key):
        """Helper function to recursively search for a key starting from the given node."""
        if node.is_leaf:
            for i, k in enumerate(node.keys):
                if k == key:
                    return node.values[i]
            return None
        else:
            # Find the correct child pointer
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            return self._search(node.children[i], key)

    # ------------------------------------------------------------------ #
    #  Insertion                                                           #
    # ------------------------------------------------------------------ #

    def insert(self, key, value):
        """Insert a new key-value pair into the B+ tree."""
        root = self.root

        # If root is full, split it first
        if root.is_full():
            new_root          = BPlusTreeNode(self.order, is_leaf=False)
            new_root.children = [self.root]
            self._split_child(new_root, 0)
            self.root = new_root

        self._insert_non_full(self.root, key, value)

    def _insert_non_full(self, node, key, value):
        """Insert key-value into a node that is not full."""
        if node.is_leaf:
            # Check for duplicate key → update
            for i, k in enumerate(node.keys):
                if k == key:
                    node.values[i] = value
                    return

            # Insert in sorted position
            i = len(node.keys) - 1
            node.keys.append(None)
            node.values.append(None)
            while i >= 0 and node.keys[i] > key:
                node.keys[i + 1]   = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            node.keys[i + 1]   = key
            node.values[i + 1] = value
        else:
            # Find child to descend into
            i = len(node.keys) - 1
            while i >= 0 and node.keys[i] > key:
                i -= 1
            i += 1  # i is now the correct child index

            if node.children[i].is_full():
                self._split_child(node, i)
                # After split, determine which of the two children to descend
                if key >= node.keys[i]:
                    i += 1

            self._insert_non_full(node.children[i], key, value)

    def _split_child(self, parent, index):
        """
        Split the child node at given index in the parent.
        This is triggered when the child is full.
        """
        order = self.order
        child = parent.children[index]
        mid   = (order - 1) // 2   # split point index

        new_node          = BPlusTreeNode(order, is_leaf=child.is_leaf)

        if child.is_leaf:
            # Leaf split: new node gets right half; separator key is new_node.keys[0]
            new_node.keys   = child.keys[mid:]
            new_node.values = child.values[mid:]
            child.keys      = child.keys[:mid]
            child.values    = child.values[:mid]
            # Maintain leaf linked list
            new_node.next   = child.next
            child.next      = new_node
            push_up_key     = new_node.keys[0]
        else:
            # Internal split: middle key is pushed up, not duplicated
            push_up_key       = child.keys[mid]
            new_node.keys     = child.keys[mid + 1:]
            new_node.children = child.children[mid + 1:]
            child.keys        = child.keys[:mid]
            child.children    = child.children[:mid + 1]

        # Insert push_up_key into parent at correct position
        parent.keys.insert(index, push_up_key)
        parent.children.insert(index + 1, new_node)

    # ------------------------------------------------------------------ #
    #  Deletion                                                            #
    # ------------------------------------------------------------------ #

    def delete(self, key):
        """Delete a key from the B+ tree. Returns True if deleted, False if not found."""
        result = self._delete(self.root, key)
        # If root is internal and empty after deletion, shrink tree
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
        return result

    def _delete(self, node, key):
        """Recursive helper function for delete operation."""
        t = (self.order - 1) // 2   # minimum keys

        if node.is_leaf:
            if key in node.keys:
                idx = node.keys.index(key)
                node.keys.pop(idx)
                node.values.pop(idx)
                return True
            return False
        else:
            # Find the child that should contain the key
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1

            # Ensure the child has at least t keys before descending
            if len(node.children[i].keys) < t:
                self._fill_child(node, i)
                # After fill, recalculate i (tree may have changed)
                i = 0
                while i < len(node.keys) and key >= node.keys[i]:
                    i += 1

            result = self._delete(node.children[i], key)

            # Update separator keys in this internal node if needed
            # (a key that was used as separator may now be deleted from leaf)
            for j in range(len(node.keys)):
                if node.keys[j] == key:
                    # Find the new leftmost key of the right subtree
                    leaf = node.children[j + 1]
                    while not leaf.is_leaf:
                        leaf = leaf.children[0]
                    if leaf.keys:
                        node.keys[j] = leaf.keys[0]
                    break

            return result

    def _fill_child(self, node, index):
        """Ensure that the child node has enough keys to allow safe deletion."""
        t = (self.order - 1) // 2

        if index > 0 and len(node.children[index - 1].keys) > t:
            self._borrow_from_prev(node, index)
        elif index < len(node.children) - 1 and len(node.children[index + 1].keys) > t:
            self._borrow_from_next(node, index)
        else:
            # Merge
            if index < len(node.children) - 1:
                self._merge(node, index)
            else:
                self._merge(node, index - 1)

    def _borrow_from_prev(self, node, index):
        """Borrow a key from the left sibling."""
        child    = node.children[index]
        left_sib = node.children[index - 1]

        if child.is_leaf:
            # Move last key/value of left sibling to front of child
            child.keys.insert(0, left_sib.keys.pop())
            child.values.insert(0, left_sib.values.pop())
            # Update separator in parent
            node.keys[index - 1] = child.keys[0]
        else:
            # Internal borrow: pull down parent key, push up left sibling's last key
            child.keys.insert(0, node.keys[index - 1])
            child.children.insert(0, left_sib.children.pop())
            node.keys[index - 1] = left_sib.keys.pop()

    def _borrow_from_next(self, node, index):
        """Borrow a key from the right sibling."""
        child     = node.children[index]
        right_sib = node.children[index + 1]

        if child.is_leaf:
            # Move first key/value of right sibling to end of child
            child.keys.append(right_sib.keys.pop(0))
            child.values.append(right_sib.values.pop(0))
            # Update separator in parent
            node.keys[index] = right_sib.keys[0]
        else:
            # Internal borrow
            child.keys.append(node.keys[index])
            child.children.append(right_sib.children.pop(0))
            node.keys[index] = right_sib.keys.pop(0)

    def _merge(self, node, index):
        """Merge two child nodes into one."""
        left  = node.children[index]
        right = node.children[index + 1]

        if left.is_leaf:
            # Merge right into left
            left.keys.extend(right.keys)
            left.values.extend(right.values)
            left.next = right.next
        else:
            # Pull down separator key then merge
            left.keys.append(node.keys[index])
            left.keys.extend(right.keys)
            left.children.extend(right.children)

        # Remove separator key and right child pointer from parent
        node.keys.pop(index)
        node.children.pop(index + 1)

    # ------------------------------------------------------------------ #
    #  Update                                                              #
    # ------------------------------------------------------------------ #

    def update(self, key, new_value):
        """Update the value associated with a key."""
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            node = node.children[i]

        for i, k in enumerate(node.keys):
            if k == key:
                node.values[i] = new_value
                return True
        return False

    # ------------------------------------------------------------------ #
    #  Range Query                                                         #
    # ------------------------------------------------------------------ #

    def range_query(self, start_key, end_key):
        """
        Return all key-value pairs where start_key <= key <= end_key.
        Utilizes the linked list structure of leaf nodes.
        """
        results = []
        # Navigate to the leaf containing start_key
        node = self.root
        while not node.is_leaf:
            i = 0
            while i < len(node.keys) and start_key >= node.keys[i]:
                i += 1
            node = node.children[i]

        # Traverse leaf linked list
        while node is not None:
            for i, k in enumerate(node.keys):
                if k > end_key:
                    return results
                if k >= start_key:
                    results.append((k, node.values[i]))
            node = node.next
        return results

    # ------------------------------------------------------------------ #
    #  Get All                                                             #
    # ------------------------------------------------------------------ #

    def get_all(self):
        """Get all key-value pairs in the tree in sorted order."""
        result = []
        self._get_all(self.root, result)
        return result

    def _get_all(self, node, result):
        """Recursive helper function to gather all key-value pairs."""
        if node.is_leaf:
            for i, k in enumerate(node.keys):
                result.append((k, node.values[i]))
        else:
            for child in node.children:
                self._get_all(child, result)

    # ------------------------------------------------------------------ #
    #  Utility                                                             #
    # ------------------------------------------------------------------ #

    def height(self):
        """Return the height of the tree."""
        node, h = self.root, 1
        while not node.is_leaf:
            node = node.children[0]
            h += 1
        return h

    def count(self):
        """Return total number of key-value pairs stored."""
        return len(self.get_all())

    def min_key(self):
        node = self.root
        while not node.is_leaf: node = node.children[0]
        return node.keys[0] if node.keys else None

    def max_key(self):
        node = self.root
        while not node.is_leaf: node = node.children[-1]
        return node.keys[-1] if node.keys else None

    # ------------------------------------------------------------------ #
    #  Visualisation                                                       #
    # ------------------------------------------------------------------ #

    def visualize_tree(self, filename=None):
        """
        Visualize the tree using graphviz.
        Optional filename can be provided to save the output.
        Internal nodes: lightblue | Leaf nodes: lightgreen
        Leaf next-pointers: dashed darkgreen
        """
        dot = Digraph()
        dot.attr(rankdir='TB', splines='line')
        dot.attr('node', fontname='Times New Roman', fontsize='12')

        if self.root.keys:
            self._add_nodes(dot, self.root)
            self._add_edges(dot, self.root)

        if filename:
            dot.render(filename, format='png', cleanup=True)

        return dot

    def _add_nodes(self, dot, node):
        """Add graph nodes for visualization."""
        node_id = str(id(node))

        if node.is_leaf:
            # Build record label: key1 | key2 | ...
            label = ' | '.join(str(k) for k in node.keys)
            dot.node(node_id, label=label, shape='record',
                     style='filled', fillcolor='lightgreen',
                     tooltip='Leaf Node')
        else:
            label = ' | '.join(str(k) for k in node.keys)
            dot.node(node_id, label=label, shape='record',
                     style='filled', fillcolor='lightblue',
                     tooltip='Internal Node')
            for child in node.children:
                self._add_nodes(dot, child)

    def _add_edges(self, dot, node):
        """Add graph edges for visualization."""
        node_id = str(id(node))

        if not node.is_leaf:
            for i, child in enumerate(node.children):
                child_id = str(id(child))
                # Label edge with the key range it covers
                if i < len(node.keys):
                    dot.edge(f'{node_id}:f{i}', child_id)
                else:
                    dot.edge(node_id, child_id)
                self._add_edges(dot, child)
        else:
            # Draw leaf linked-list next pointers
            if node.next is not None:
                next_id = str(id(node.next))
                dot.edge(node_id, next_id,
                         style='dashed', color='darkgreen',
                         tooltip='Next Leaf', constraint='false')
