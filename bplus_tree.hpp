#ifndef BPLUS_TREE_HPP
#define BPLUS_TREE_HPP

#include <vector>
#include <algorithm>
#include <memory>
#include <utility>
#include <shared_mutex>

// Simple in-memory B+ Tree implementation for efficient time range queries
template <typename K, typename V>
class BPlusTree {
private:
    // Constants
    static constexpr int ORDER = 64; // High order for better cache efficiency
    
    // Node structure
    struct Node {
        bool is_leaf;
        std::vector<K> keys;
        
        Node(bool leaf) : is_leaf(leaf) {}
        virtual ~Node() = default;
    };
    
    struct InternalNode : public Node {
        std::vector<std::unique_ptr<Node>> children;
        
        InternalNode() : Node(false) {}
    };
    
    struct LeafNode : public Node {
        std::vector<V> values;
        LeafNode* next; // For range queries
        
        LeafNode() : Node(true), next(nullptr) {}
    };
    
    std::unique_ptr<Node> root;
    mutable std::shared_mutex mutex; // Reader-writer lock for concurrent access
    
public:
    BPlusTree() : root(std::make_unique<LeafNode>()) {}
    
    // Insert a key-value pair
    void insert(const K& key, const V& value) {
        std::unique_lock<std::shared_mutex> lock(mutex);
        
        if (root->is_leaf) {
            auto leaf = static_cast<LeafNode*>(root.get());
            
            // Find position to insert
            auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
            auto index = std::distance(leaf->keys.begin(), it);
            
            // Insert key and value
            leaf->keys.insert(it, key);
            leaf->values.insert(leaf->values.begin() + index, value);
            
            // Split if necessary
            if (leaf->keys.size() >= ORDER) {
                split_leaf(leaf);
            }
        } else {
            insert_internal(key, value, static_cast<InternalNode*>(root.get()));
        }
    }
    
    // Find values within a range [start, end]
    std::vector<std::pair<K, V>> range_query(const K& start, const K& end) const {
        std::shared_lock<std::shared_mutex> lock(mutex);
        std::vector<std::pair<K, V>> result;
        
        // Find the leaf containing the start key
        LeafNode* leaf = find_leaf(start);
        
        // Iterate through leaves collecting values in range
        while (leaf != nullptr) {
            for (size_t i = 0; i < leaf->keys.size(); ++i) {
                if (leaf->keys[i] >= start && leaf->keys[i] <= end) {
                    result.emplace_back(leaf->keys[i], leaf->values[i]);
                }
                if (leaf->keys[i] > end) {
                    return result;
                }
            }
            leaf = leaf->next;
        }
        
        return result;
    }
    
private:
    // Find the leaf node that should contain key
    LeafNode* find_leaf(const K& key) const {
        Node* current = root.get();
        
        while (!current->is_leaf) {
            auto internal = static_cast<InternalNode*>(current);
            auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
            auto index = std::distance(internal->keys.begin(), it);
            current = internal->children[index].get();
        }
        
        return static_cast<LeafNode*>(current);
    }
    
    // Insert into internal node
    void insert_internal(const K& key, const V& value, InternalNode* node) {
        // Find child to insert into
        auto it = std::upper_bound(node->keys.begin(), node->keys.end(), key);
        auto index = std::distance(node->keys.begin(), it);
        
        auto child = node->children[index].get();
        
        if (child->is_leaf) {
            auto leaf = static_cast<LeafNode*>(child);
            
            // Insert into leaf
            auto leaf_it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
            auto leaf_index = std::distance(leaf->keys.begin(), leaf_it);
            
            leaf->keys.insert(leaf_it, key);
            leaf->values.insert(leaf->values.begin() + leaf_index, value);
            
            // Split if necessary
            if (leaf->keys.size() >= ORDER) {
                split_leaf_with_parent(leaf, node, index);
            }
        } else {
            insert_internal(key, value, static_cast<InternalNode*>(child));
        }
    }
    
    // Split the root when it's a leaf
    void split_leaf(LeafNode* leaf) {
        auto new_root = std::make_unique<InternalNode>();
        auto new_leaf = std::make_unique<LeafNode>();
        
        // Middle position
        size_t mid = leaf->keys.size() / 2;
        
        // Copy keys and values to new leaf
        new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
        new_leaf->values.assign(leaf->values.begin() + mid, leaf->values.end());
        
        // Update old leaf
        leaf->keys.resize(mid);
        leaf->values.resize(mid);
        
        // Link leaves for range queries
        new_leaf->next = leaf->next;
        leaf->next = new_leaf.get();
        
        // Update root
        new_root->keys.push_back(new_leaf->keys.front());
        new_root->children.push_back(std::make_unique<LeafNode>(*leaf));
        new_root->children.push_back(std::move(new_leaf));
        
        root = std::move(new_root);
    }
    
    // Split a leaf with parent
    void split_leaf_with_parent(LeafNode* leaf, InternalNode* parent, size_t child_index) {
        auto new_leaf = std::make_unique<LeafNode>();
        
        // Middle position
        size_t mid = leaf->keys.size() / 2;
        
        // Copy keys and values to new leaf
        new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
        new_leaf->values.assign(leaf->values.begin() + mid, leaf->values.end());
        
        // Update old leaf
        leaf->keys.resize(mid);
        leaf->values.resize(mid);
        
        // Link leaves for range queries
        new_leaf->next = leaf->next;
        leaf->next = new_leaf.get();
        
        // Insert new leaf into parent
        parent->keys.insert(parent->keys.begin() + child_index, new_leaf->keys.front());
        parent->children.insert(parent->children.begin() + child_index + 1, std::move(new_leaf));
        
        // Split parent if necessary
        if (parent->keys.size() >= ORDER) {
            split_internal(parent);
        }
    }
    
    // Split an internal node
    void split_internal(InternalNode* node) {
        if (node == root.get()) {
            auto new_root = std::make_unique<InternalNode>();
            auto new_internal = std::make_unique<InternalNode>();
            
            // Middle position
            size_t mid = node->keys.size() / 2;
            K middle_key = node->keys[mid];
            
            // Copy keys and children to new internal node
            new_internal->keys.assign(node->keys.begin() + mid + 1, node->keys.end());
            
            // Move children to new internal node
            for (size_t i = mid + 1; i <= node->children.size() - 1; ++i) {
                new_internal->children.push_back(std::move(node->children[i]));
            }
            
            // Update old internal node
            node->keys.resize(mid);
            node->children.resize(mid + 1);
            
            // Update root
            new_root->keys.push_back(middle_key);
            new_root->children.push_back(std::make_unique<InternalNode>(std::move(*node)));
            new_root->children.push_back(std::move(new_internal));
            
            root = std::move(new_root);
        } else {
            // TODO: Implement internal node split with parent
            // For the current scope, we'll assume the tree depth is limited
        }
    }
};

#endif // BPLUS_TREE_HPP