#ifndef CAMPUS_NODE_H
#define CAMPUS_NODE_H

#include "version.h"
#include <vector>
#include <cassert>

class Node {
public:
    Node(int max_posting_size, int dimension, size_t element_size)
        : archived_(false), version_count_(0),
            latest_version_(new Version(0, this, nullptr, max_posting_size, dimension, element_size)) {};

    Version *getLatestVersion() const { return latest_version_; }
    Node *getPrevNode() const { return prev_node_; }
    bool isArchived() const { return archived_; }
    void addNeighbor(int neighbor_id);
    void setPrevNode(Node *prev_node) { prev_node_ = prev_node; }
    void setArchived() { archived_ = true; }
    void switchVersion(Version *new_version){
        latest_version_ = new_version;
    };
    std::vector<int> getNeighbors() const;

private:
    bool archived_;
    int version_count_;
    Version *latest_version_;
    Node *prev_node_;
};

#endif //CAMPUS_NODE_H