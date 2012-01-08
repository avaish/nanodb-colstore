package edu.caltech.nanodb.plans;


import java.util.ArrayList;


/**
 * Represents a tree traversal path for a PlanNode.
 *
 * @review (Donnie) I really don't know what this is for!  Look at removing...
 */
public class NodePath {
    /** Typesafe enumeration for tree traversal directions. */
    private enum Direction {
        /** Represents traversing left through the tree. */
        LEFT,

        /** Represents traversing right through the tree. */
        RIGHT
    };


    /** List of traversal instructions. */
    private ArrayList<Direction> path;


    /**
     * Creates an empty node path.
     */
    public NodePath() {
        this.path = new ArrayList<Direction>();
    }


    /**
     * Creates a new node path that copies another node path.
     *
     * @param nodePath the path to copy.
     */
    @SuppressWarnings("unchecked")
    public NodePath(NodePath nodePath) {
        this.path = (ArrayList<Direction>) nodePath.path.clone();
    }
  
  
    /** Appends a LEFT to the traversal. */
    public void walkLeft() {
        path.add(Direction.LEFT);
    }
  
  
    /** Appends a RIGHT to the traversal. */
    public void walkRight() {
        path.add(Direction.RIGHT);
    }


    /** Removes the last direction. */
    public void backUp() {
        int length = path.size();
        path.remove(length - 1);
    }


    /**
     * Traverses a node tree using the path specified by this object.
     *
     * @param node The node tree to traverse.
     * @return The target node after taking this path
     * @throws java.lang.NullPointerException if an unexpected null child was
     * found
     */
    public PlanNode traverse(PlanNode node) throws NullPointerException {

        // Traverse the node tree by looking at each direction in sequence.
        for (Direction dir : path) {
            if (dir == Direction.LEFT) {
                // Check to see if the left child is not null.
                if (node.leftChild == null) {
                    throw new NullPointerException(
                        "Null left child found in left tree traversal at node:  " +
                        node);
                }
                else {
                    // Set the node reference to the left child.
                    node = node.leftChild;
                }
            }
            else {
                // We are going right
                // Check to see if the right child is not null.
                if (node.rightChild == null) {
                    throw new NullPointerException(
                        "Null right child found in right tree traversal at node:  " +
                        node);
                }
                else {
                    // Set the node reference to the right child.
                    node = node.rightChild;
                }
            }
        }
    
        return node;
    }


    /** Returns the node path as a string. */
    @Override
    public String toString() {
        return path.toString();
    }
}
