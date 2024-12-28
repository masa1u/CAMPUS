#include "node.h"

Version *Node::getLatestVersion() {
    return latest_version_;
}

void Node::addNeighbor(int neighbor_id) {
    neighbors_.push_back(neighbor_id);
}

std::vector<int> Node::getNeighbors() const {
    return neighbors_;
}