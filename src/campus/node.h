#ifndef CAMPUS_NODE_H
#define CAMPUS_NODE_H

#include "version.h"
#include <vector>

class Node {
public:
    Node(int node_id, int max_posting_size, int dimension, size_t element_size)
        : node_id_(node_id), version_count_(0), latest_version_(new Version(0, max_posting_size, dimension, element_size)) {};

    Version *getLatestVersion();
    int getNodeId() const { return node_id_; }
    void addNeighbor(int neighbor_id);
    std::vector<int> getNeighbors() const;

private:
    const int node_id_;
    int version_count_;
    Version *latest_version_;
    std::vector<int> neighbors_;
};

#endif //CAMPUS_NODE_H