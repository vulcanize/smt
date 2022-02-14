package smt

import (
	"bytes"
	"errors"
	"hash"
)

// ErrBadProof is returned when an invalid Merkle proof is supplied.
var ErrBadProof = errors.New("bad proof")

// DeepSparseMerkleSubTree is a deep Sparse Merkle subtree for working on only a few leafs.
type DeepSparseMerkleSubTree struct {
	*SparseMerkleTree
}

// NewDeepSparseMerkleSubTree creates a new deep Sparse Merkle subtree on an empty MapStore.
func NewDeepSparseMerkleSubTree(nodes, values MapStore, hasher hash.Hash, root []byte) *DeepSparseMerkleSubTree {
	return &DeepSparseMerkleSubTree{
		SparseMerkleTree: ImportSparseMerkleTree(nodes, values, hasher, root),
	}
}

// AddBranch adds a branch to the tree.
// These branches are generated by smt.ProveForRoot.
// If the proof is invalid, a ErrBadProof is returned.
//
// If the leaf may be updated (e.g. during a state transition fraud proof),
// an updatable proof should be used. See SparseMerkleTree.ProveUpdatable.
func (dsmst *DeepSparseMerkleSubTree) AddBranch(proof SparseMerkleProof, key []byte, value []byte) error {
	result, updates := verifyProofWithUpdates(proof, dsmst.Root(), key, value, dsmst.th.hasher)
	if !result {
		return ErrBadProof
	}

	if !bytes.Equal(value, defaultValue) { // Membership proof.
		if err := dsmst.dirtyValues.Set(dsmst.th.path(key), value); err != nil {
			return err
		}
	}

	// Update nodes along branch
	for _, update := range updates {
		err := dsmst.dirtyNodes.Set(update[0], update[1])
		if err != nil {
			return err
		}
	}

	// Update sibling node
	if proof.SiblingData != nil {
		if proof.SideNodes != nil && len(proof.SideNodes) > 0 {
			err := dsmst.dirtyNodes.Set(proof.SideNodes[0], proof.SiblingData)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// GetDescend gets the value of a key from the tree by descending it.
// Use if a key was _not_ previously added with AddBranch, otherwise use Get.
// Errors if the key cannot be reached by descending.
func (smt *SparseMerkleTree) GetDescend(key []byte) ([]byte, error) {
	// Get tree's root
	root := smt.Root()

	if bytes.Equal(root, smt.th.placeholder()) {
		// The tree is empty, return the default value.
		return defaultValue, nil
	}

	path := smt.th.path(key)
	currentHash := root
	for i := 0; i < smt.depth(); i++ {
		// Try dirty cache first
		var currentData []byte
		var err error
		currentData, err = smt.dirtyNodes.Get(currentHash)
		if err != nil {
			currentData, err = smt.nodes.Get(currentHash)
			if err != nil {
				return nil, err
			}
		}
		if smt.th.isLeaf(currentData) {
			// We've reached the end. Is this the actual leaf?
			p, _ := smt.th.parseLeaf(currentData)
			if !bytes.Equal(path, p) {
				// Nope. Therefore the key is actually empty.
				return defaultValue, nil
			}
			// Otherwise, yes. Return the value.
			// Try dirty cache first
			var value []byte
			value, err = smt.dirtyValues.Get(path)
			if err != nil {
				value, err = smt.values.Get(path)
				if err != nil {
					return nil, err
				}
			}
			return value, nil
		}

		leftNode, rightNode := smt.th.parseNode(currentData)
		if getBitAtFromMSB(path, i) == right {
			currentHash = rightNode
		} else {
			currentHash = leftNode
		}

		if bytes.Equal(currentHash, smt.th.placeholder()) {
			// We've hit a placeholder value; this is the end.
			return defaultValue, nil
		}
	}

	// The following lines of code should only be reached if the path is 256
	// nodes high, which should be very unlikely if the underlying hash function
	// is collision-resistant.
	// Try dirty cache first
	var value []byte
	var err error
	value, err = smt.dirtyValues.Get(path)
	if err != nil {
		value, err = smt.values.Get(path)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

// HasDescend returns true if the value at the given key is non-default, false
// otherwise.
// Use if a key was _not_ previously added with AddBranch, otherwise use Has.
// Errors if the key cannot be reached by descending.
func (smt *SparseMerkleTree) HasDescend(key []byte) (bool, error) {
	val, err := smt.GetDescend(key)
	if err != nil {
		return false, err
	}
	return !bytes.Equal(defaultValue, val), nil
}
