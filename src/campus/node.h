#ifndef CAMPUS_NODE_H
#define CAMPUS_NODE_H

#include "version.h"
#include <vector>

class Node {
public:
    Node(int node_id, int max_posting_size, int dimension, size_t element_size)
        : archived_(false), node_id_(node_id), version_count_(0),
            latest_version_(new Version(node_id, 0, nullptr, max_posting_size, dimension, element_size)) {};

    Version *getLatestVersion() const { return latest_version_; }
    int getNodeId() const { return node_id_; }
    bool isArchived() const { return archived_; }
    void addNeighbor(int neighbor_id);
    std::vector<int> getNeighbors() const;

private:
    bool archived_;
    const int node_id_;
    int version_count_;
    Version *latest_version_;
};

#endif //CAMPUS_NODE_H