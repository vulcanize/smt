// Package smt implements a Sparse Merkle tree.
package smt

import (
	"bytes"
	"errors"
	"hash"
)

const (
	right = 1
)

var defaultValue = []byte{}

var errKeyAlreadyEmpty = errors.New("key already empty")

// SparseMerkleTree is a Sparse Merkle tree.
// We need to be able to track which nodes have been deleted in the cache, and echo these
// deletions back to the disk layer
// We also need to be wary of the scenario where within the same cache/commit block we
// delete and set the same key multiple times, we need to only reflect the latest state of those keys
type SparseMerkleTree struct {
	th            treeHasher
	nodes, values MapStore
	// TODO: make caching more intelligent, be able to configure a cache size and if we exceed this size auto-flush to disk
	dirtyNodes, dirtyValues             *SimpleMap
	dirtyNodeDeletes, dirtyValueDeletes map[string]struct{}
	root                                []byte
	nodesListen                         chan Node
	kvListen                            chan KVPair
}

// Node struct for listening to nodes
// Delete flag is included so that we can signify to an external service when to delete a given node
type Node struct {
	Hash    []byte
	Content []byte
	Delete  bool
}

// KVPair struct for listening to KVPairs
// Delete flag is included so that we can signify to an external service when to delete a given KVPair
type KVPair struct {
	Key    string
	Value  []byte
	Delete bool
}

// NewSparseMerkleTree creates a new Sparse Merkle tree on an empty MapStore.
func NewSparseMerkleTree(nodes, values MapStore, hasher hash.Hash, options ...Option) *SparseMerkleTree {
	smt := SparseMerkleTree{
		th:     *newTreeHasher(hasher),
		nodes:  nodes,
		values: values,
	}

	for _, option := range options {
		option(&smt)
	}

	smt.SetRoot(smt.th.placeholder())

	return &smt
}

// ImportSparseMerkleTree imports a Sparse Merkle tree from a non-empty MapStore.
func ImportSparseMerkleTree(nodes, values MapStore, hasher hash.Hash, root []byte) *SparseMerkleTree {
	smt := SparseMerkleTree{
		th:     *newTreeHasher(hasher),
		nodes:  nodes,
		values: values,
		root:   root,
	}
	return &smt
}

// Root gets the root of the tree.
func (smt *SparseMerkleTree) Root() []byte {
	return smt.root
}

// SetRoot sets the root of the tree.
func (smt *SparseMerkleTree) SetRoot(root []byte) {
	smt.root = root
}

func (smt *SparseMerkleTree) depth() int {
	return smt.th.pathSize() * 8
}

// Get gets the value of a key from the tree.
func (smt *SparseMerkleTree) Get(key []byte) ([]byte, error) {
	// Get tree's root
	root := smt.Root()

	if bytes.Equal(root, smt.th.placeholder()) {
		// The tree is empty, return the default value.
		return defaultValue, nil
	}

	// Try dirty cache first
	path := smt.th.path(key)
	var value []byte
	var err error
	value, err = smt.dirtyValues.Get(path)
	if err != nil {
		value, err = smt.values.Get(path)
		if err != nil {
			var invalidKeyError *InvalidKeyError

			if errors.As(err, &invalidKeyError) {
				// If key isn't found, return default value
				return defaultValue, nil
			} else {
				// Otherwise percolate up any other error
				return nil, err
			}
		}
	}
	return value, nil
}

// Has returns true if the value at the given key is non-default, false
// otherwise.
func (smt *SparseMerkleTree) Has(key []byte) (bool, error) {
	val, err := smt.Get(key)
	return !bytes.Equal(defaultValue, val), err
}

// Commit flushes all dirty nodes and values to the backing (persistent) MapStore
// and, if the SMT is configured with listeners, it streams nodes and kvPairs to the listeners
// this dirty streaming enables us to perform efficient statediffing- efficient export of SC + SS to an external service
func (smt *SparseMerkleTree) Commit() error {
	// I know this is ugly code replication, but it's better to perform two nil checks
	// than a nil check on each iteration of these maps
	if smt.kvListen == nil {
		for key, val := range smt.dirtyValues.m {
			if err := smt.values.Set([]byte(key), val); err != nil {
				return err
			}
		}
		for key := range smt.dirtyNodeDeletes {
			if err := smt.values.Delete([]byte(key)); err != nil {
				return err
			}
		}
	} else {
		for key, val := range smt.dirtyValues.m {
			if err := smt.values.Set([]byte(key), val); err != nil {
				return err
			}
			smt.kvListen <- KVPair{key, val, false}
		}
		for key := range smt.dirtyNodeDeletes {
			if err := smt.values.Delete([]byte(key)); err != nil {
				return err
			}
			smt.kvListen <- KVPair{key, nil, true}
		}
	}
	if smt.nodesListen == nil {
		for key, val := range smt.dirtyNodes.m {
			if err := smt.nodes.Set([]byte(key), val); err != nil {
				return err
			}
		}
		for key := range smt.dirtyNodeDeletes {
			if err := smt.nodes.Delete([]byte(key)); err != nil {
				return err
			}
		}
	} else {
		for key, val := range smt.dirtyNodes.m {
			if err := smt.nodes.Set([]byte(key), val); err != nil {
				return err
			}
			smt.nodesListen <- Node{[]byte(key), val, false}
		}
		for key := range smt.dirtyNodeDeletes {
			if err := smt.nodes.Delete([]byte(key)); err != nil {
				return err
			}
			smt.nodesListen <- Node{[]byte(key), nil, true}
		}
	}
	return nil
}

// SetNodeListener to configure the SMT with a channel to write dirty nodes out to during Commit
func (smt *SparseMerkleTree) SetNodeListener(listener chan Node) {
	smt.nodesListen = listener
}

// SetKVListener to configure the SMT with a channel to write dirty kv pairs out to during Commit
func (smt *SparseMerkleTree) SetKVListener(listener chan KVPair) {
	smt.kvListen = listener
}

// Update sets a new value for a key in the tree, and sets and returns the new root of the tree.
func (smt *SparseMerkleTree) Update(key []byte, value []byte) ([]byte, error) {
	newRoot, err := smt.UpdateForRoot(key, value, smt.Root())
	if err != nil {
		return nil, err
	}
	smt.SetRoot(newRoot)
	return newRoot, nil
}

// Delete deletes a value from tree. It returns the new root of the tree.
func (smt *SparseMerkleTree) Delete(key []byte) ([]byte, error) {
	return smt.Update(key, defaultValue)
}

// UpdateForRoot sets a new value for a key in the tree at a specific root, and returns the new root.
func (smt *SparseMerkleTree) UpdateForRoot(key []byte, value []byte, root []byte) ([]byte, error) {
	path := smt.th.path(key)
	sideNodes, pathNodes, oldLeafData, _, err := smt.sideNodesForRoot(path, root, false)
	if err != nil {
		return nil, err
	}

	var newRoot []byte
	if bytes.Equal(value, defaultValue) {
		// Delete operation.
		newRoot, err = smt.deleteWithSideNodes(path, sideNodes, pathNodes, oldLeafData)
		if errors.Is(err, errKeyAlreadyEmpty) {
			// This key is already empty; return the old root.
			return root, nil
		}
		// Try to delete from dirty cache first
		if err := smt.dirtyValues.Delete(path); err != nil {
			if err := smt.values.Delete(path); err != nil {
				return nil, err
			}
		}
		// Cache the delete operation so that listeners can be made aware of them
		smt.dirtyValueDeletes[string(path)] = struct{}{}

	} else {
		// Insert or update operation.
		newRoot, err = smt.updateWithSideNodes(path, value, sideNodes, pathNodes, oldLeafData)
	}
	return newRoot, err
}

// DeleteForRoot deletes a value from tree at a specific root. It returns the new root of the tree.
func (smt *SparseMerkleTree) DeleteForRoot(key, root []byte) ([]byte, error) {
	return smt.UpdateForRoot(key, defaultValue, root)
}

func (smt *SparseMerkleTree) deleteWithSideNodes(path []byte, sideNodes [][]byte, pathNodes [][]byte, oldLeafData []byte) ([]byte, error) {
	if bytes.Equal(pathNodes[0], smt.th.placeholder()) {
		// This key is already empty as it is a placeholder; return an error.
		return nil, errKeyAlreadyEmpty
	}
	actualPath, _ := smt.th.parseLeaf(oldLeafData)
	if !bytes.Equal(path, actualPath) {
		// This key is already empty as a different key was found its place; return an error.
		return nil, errKeyAlreadyEmpty
	}
	// All nodes above the deleted leaf are now orphaned
	for _, node := range pathNodes {
		// Try to delete from dirty cache first
		if err := smt.dirtyNodes.Delete(node); err != nil {
			if err := smt.nodes.Delete(node); err != nil {
				return nil, err
			}
		}
		// Cache the delete operation so that listeners can be made aware of them
		smt.dirtyNodeDeletes[string(node)] = struct{}{}
	}

	var currentHash, currentData []byte
	nonPlaceholderReached := false
	for i, sideNode := range sideNodes {
		if currentData == nil {
			// Check dirty cache first
			var sideNodeValue []byte
			var err error
			sideNodeValue, err = smt.dirtyNodes.Get(sideNode)
			if err != nil {
				sideNodeValue, err = smt.nodes.Get(sideNode)
				if err != nil {
					return nil, err
				}
			}
			if smt.th.isLeaf(sideNodeValue) {
				// This is the leaf sibling that needs to be bubbled up the tree.
				currentHash = sideNode
				currentData = sideNode
				continue
			} else {
				// This is the node sibling that needs to be left in its place.
				currentData = smt.th.placeholder()
				nonPlaceholderReached = true
			}
		}

		if !nonPlaceholderReached && bytes.Equal(sideNode, smt.th.placeholder()) {
			// We found another placeholder sibling node, keep going up the
			// tree until we find the first sibling that is not a placeholder.
			continue
		} else if !nonPlaceholderReached {
			// We found the first sibling node that is not a placeholder, it is
			// time to insert our leaf sibling node here.
			nonPlaceholderReached = true
		}

		if getBitAtFromMSB(path, len(sideNodes)-1-i) == right {
			currentHash, currentData = smt.th.digestNode(sideNode, currentData)
		} else {
			currentHash, currentData = smt.th.digestNode(currentData, sideNode)
		}
		if err := smt.dirtyNodes.Set(currentHash, currentData); err != nil {
			return nil, err
		}
		// Remove this key from the deleted cache if it exists
		delete(smt.dirtyNodeDeletes, string(currentHash))
		currentData = currentHash
	}

	if currentHash == nil {
		// The tree is empty; return placeholder value as root.
		currentHash = smt.th.placeholder()
	}
	return currentHash, nil
}

func (smt *SparseMerkleTree) updateWithSideNodes(path []byte, value []byte, sideNodes [][]byte, pathNodes [][]byte, oldLeafData []byte) ([]byte, error) {
	valueHash := smt.th.digest(value)
	currentHash, currentData := smt.th.digestLeaf(path, valueHash)
	if err := smt.dirtyNodes.Set(currentHash, currentData); err != nil {
		return nil, err
	}
	// Remove this key from the deleted cache if it exists
	delete(smt.dirtyNodeDeletes, string(currentHash))
	currentData = currentHash

	// If the leaf node that sibling nodes lead to has a different actual path
	// than the leaf node being updated, we need to create an intermediate node
	// with this leaf node and the new leaf node as children.
	//
	// First, get the number of bits that the paths of the two leaf nodes share
	// in common as a prefix.
	var commonPrefixCount int
	var oldValueHash []byte
	if bytes.Equal(pathNodes[0], smt.th.placeholder()) {
		commonPrefixCount = smt.depth()
	} else {
		var actualPath []byte
		actualPath, oldValueHash = smt.th.parseLeaf(oldLeafData)
		commonPrefixCount = countCommonPrefix(path, actualPath)
	}
	if commonPrefixCount != smt.depth() {
		if getBitAtFromMSB(path, commonPrefixCount) == right {
			currentHash, currentData = smt.th.digestNode(pathNodes[0], currentData)
		} else {
			currentHash, currentData = smt.th.digestNode(currentData, pathNodes[0])
		}

		err := smt.dirtyNodes.Set(currentHash, currentData)
		if err != nil {
			return nil, err
		}
		// Remove this key from the deleted cache if it exists
		delete(smt.dirtyNodeDeletes, string(currentHash))

		currentData = currentHash
	} else if oldValueHash != nil {
		// Short-circuit if the same value is being set
		if bytes.Equal(oldValueHash, valueHash) {
			return smt.root, nil
		}
		// If an old leaf exists, remove it
		// Try the dirty caches first
		if err := smt.dirtyNodes.Delete(pathNodes[0]); err != nil {
			if err := smt.nodes.Delete(pathNodes[0]); err != nil {
				return nil, err
			}
		}
		// Cache the delete operation so that listeners can be made aware of them
		smt.dirtyNodeDeletes[string(pathNodes[0])] = struct{}{}
		if err := smt.dirtyValues.Delete(path); err != nil {
			if err := smt.values.Delete(path); err != nil {
				return nil, err
			}
		}
		// Cache the delete operation so that listeners can be made aware of them
		smt.dirtyValueDeletes[string(path)] = struct{}{}
	}
	// All remaining path nodes are orphaned
	for i := 1; i < len(pathNodes); i++ {
		// Try the dirty cache first
		// TODO: I think we need a separate cache for Deletes, because there won't be an entry in the cache map for them when we iterate to flush to disk
		if err := smt.dirtyNodes.Delete(pathNodes[i]); err != nil {
			if err := smt.nodes.Delete(pathNodes[i]); err != nil {
				return nil, err
			}
		}
		// Cache the delete operation so that listeners can be made aware of them
		smt.dirtyNodeDeletes[string(pathNodes[i])] = struct{}{}
	}

	// The offset from the bottom of the tree to the start of the side nodes.
	// Note: i-offsetOfSideNodes is the index into sideNodes[]
	offsetOfSideNodes := smt.depth() - len(sideNodes)

	for i := 0; i < smt.depth(); i++ {
		var sideNode []byte

		if i-offsetOfSideNodes < 0 || sideNodes[i-offsetOfSideNodes] == nil {
			if commonPrefixCount != smt.depth() && commonPrefixCount > smt.depth()-1-i {
				// If there are no sidenodes at this height, but the number of
				// bits that the paths of the two leaf nodes share in common is
				// greater than this depth, then we need to build up the tree
				// to this depth with placeholder values at siblings.
				sideNode = smt.th.placeholder()
			} else {
				continue
			}
		} else {
			sideNode = sideNodes[i-offsetOfSideNodes]
		}

		if getBitAtFromMSB(path, smt.depth()-1-i) == right {
			currentHash, currentData = smt.th.digestNode(sideNode, currentData)
		} else {
			currentHash, currentData = smt.th.digestNode(currentData, sideNode)
		}
		err := smt.dirtyNodes.Set(currentHash, currentData)
		if err != nil {
			return nil, err
		}
		// Remove this key from the deleted cache if it exists
		delete(smt.dirtyNodeDeletes, string(currentHash))
		currentData = currentHash
	}
	if err := smt.dirtyValues.Set(path, value); err != nil {
		return nil, err
	}
	// Remove this key from the deleted cache if it exists
	delete(smt.dirtyValueDeletes, string(path))

	return currentHash, nil
}

// Get all the sibling nodes (sidenodes) for a given path from a given root.
// Returns an array of sibling nodes, the leaf hash found at that path, the
// leaf data, and the sibling data.
//
// If the leaf is a placeholder, the leaf data is nil.
func (smt *SparseMerkleTree) sideNodesForRoot(path []byte, root []byte, getSiblingData bool) ([][]byte, [][]byte, []byte, []byte, error) {
	// Side nodes for the path. Nodes are inserted in reverse order, then the
	// slice is reversed at the end.
	sideNodes := make([][]byte, 0, smt.depth())
	pathNodes := make([][]byte, 0, smt.depth()+1)
	pathNodes = append(pathNodes, root)

	if bytes.Equal(root, smt.th.placeholder()) {
		// If the root is a placeholder, there are no sidenodes to return.
		// Let the "actual path" be the input path.
		return sideNodes, pathNodes, nil, nil, nil
	}

	// Try dirty cache first
	var currentData []byte
	var err error
	currentData, err = smt.dirtyNodes.Get(root)
	if err != nil {
		currentData, err = smt.nodes.Get(root)
		if err != nil {
			return nil, nil, nil, nil, err
		}
	}
	if smt.th.isLeaf(currentData) {
		// If the root is a leaf, there are also no sidenodes to return.
		return sideNodes, pathNodes, currentData, nil, nil
	}

	var nodeHash []byte
	var sideNode []byte
	var siblingData []byte
	for i := 0; i < smt.depth(); i++ {
		leftNode, rightNode := smt.th.parseNode(currentData)

		// Get sidenode depending on whether the path bit is on or off.
		if getBitAtFromMSB(path, i) == right {
			sideNode = leftNode
			nodeHash = rightNode
		} else {
			sideNode = rightNode
			nodeHash = leftNode
		}
		sideNodes = append(sideNodes, sideNode)
		pathNodes = append(pathNodes, nodeHash)

		if bytes.Equal(nodeHash, smt.th.placeholder()) {
			// If the node is a placeholder, we've reached the end.
			currentData = nil
			break
		}
		// Try dirty cache first
		currentData, err = smt.dirtyNodes.Get(nodeHash)
		if err != nil {
			currentData, err = smt.nodes.Get(nodeHash)
			if err != nil {
				return nil, nil, nil, nil, err
			}
		}
		if smt.th.isLeaf(currentData) {
			// If the node is a leaf, we've reached the end.
			break
		}
	}

	if getSiblingData {
		// Try dirty cache first
		siblingData, err = smt.dirtyNodes.Get(sideNode)
		if err != nil {
			siblingData, err = smt.nodes.Get(sideNode)
			if err != nil {
				return nil, nil, nil, nil, err
			}
		}
	}
	return reverseByteSlices(sideNodes), reverseByteSlices(pathNodes), currentData, siblingData, nil
}

// Prove generates a Merkle proof for a key against the current root.
//
// This proof can be used for read-only applications, but should not be used if
// the leaf may be updated (e.g. in a state transition fraud proof). For
// updatable proofs, see ProveUpdatable.
func (smt *SparseMerkleTree) Prove(key []byte) (SparseMerkleProof, error) {
	proof, err := smt.ProveForRoot(key, smt.Root())
	return proof, err
}

// ProveForRoot generates a Merkle proof for a key, against a specific node.
// This is primarily useful for generating Merkle proofs for subtrees.
//
// This proof can be used for read-only applications, but should not be used if
// the leaf may be updated (e.g. in a state transition fraud proof). For
// updatable proofs, see ProveUpdatableForRoot.
func (smt *SparseMerkleTree) ProveForRoot(key []byte, root []byte) (SparseMerkleProof, error) {
	return smt.doProveForRoot(key, root, false)
}

// ProveUpdatable generates an updatable Merkle proof for a key against the current root.
func (smt *SparseMerkleTree) ProveUpdatable(key []byte) (SparseMerkleProof, error) {
	proof, err := smt.ProveUpdatableForRoot(key, smt.Root())
	return proof, err
}

// ProveUpdatableForRoot generates an updatable Merkle proof for a key, against a specific node.
// This is primarily useful for generating Merkle proofs for subtrees.
func (smt *SparseMerkleTree) ProveUpdatableForRoot(key []byte, root []byte) (SparseMerkleProof, error) {
	return smt.doProveForRoot(key, root, true)
}

func (smt *SparseMerkleTree) doProveForRoot(key []byte, root []byte, isUpdatable bool) (SparseMerkleProof, error) {
	path := smt.th.path(key)
	sideNodes, pathNodes, leafData, siblingData, err := smt.sideNodesForRoot(path, root, isUpdatable)
	if err != nil {
		return SparseMerkleProof{}, err
	}

	var nonEmptySideNodes [][]byte
	for _, v := range sideNodes {
		if v != nil {
			nonEmptySideNodes = append(nonEmptySideNodes, v)
		}
	}

	// Deal with non-membership proofs. If the leaf hash is the placeholder
	// value, we do not need to add anything else to the proof.
	var nonMembershipLeafData []byte
	if !bytes.Equal(pathNodes[0], smt.th.placeholder()) {
		actualPath, _ := smt.th.parseLeaf(leafData)
		if !bytes.Equal(actualPath, path) {
			// This is a non-membership proof that involves showing a different leaf.
			// Add the leaf data to the proof.
			nonMembershipLeafData = leafData
		}
	}

	proof := SparseMerkleProof{
		SideNodes:             nonEmptySideNodes,
		NonMembershipLeafData: nonMembershipLeafData,
		SiblingData:           siblingData,
	}

	return proof, err
}

// ProveCompact generates a compacted Merkle proof for a key against the current root.
func (smt *SparseMerkleTree) ProveCompact(key []byte) (SparseCompactMerkleProof, error) {
	proof, err := smt.ProveCompactForRoot(key, smt.Root())
	return proof, err
}

// ProveCompactForRoot generates a compacted Merkle proof for a key, at a specific root.
func (smt *SparseMerkleTree) ProveCompactForRoot(key []byte, root []byte) (SparseCompactMerkleProof, error) {
	proof, err := smt.ProveForRoot(key, root)
	if err != nil {
		return SparseCompactMerkleProof{}, err
	}
	compactedProof, err := CompactProof(proof, smt.th.hasher)
	return compactedProof, err
}
