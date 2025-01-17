#ifndef NOCONTROL_NODE_H
#define NOCONTROL_NODE_H

#include "entity.h"
#include <vector>

class Node {
public:
    Node(int max_num, int dimension, size_t element_size)
        : max_num_(max_num), dimension_(dimension), element_size_(element_size)
    {
        posting_ = new Entity*[max_num_];
        centroid = new char[dimension_ * element_size_];
    }

    ~Node() {}

    bool canAddVector() const { return vector_num_ < max_num_; }
    void addVector(const void *vector, const int vector_id);
    void deleteVector(int vector_id);
    int getVectorNum() const { return vector_num_; }
    void *getCentroid() const { return centroid; }
    std::vector<Node*> getInNeighbors() const { return in_neighbors_; }
    std::vector<Node*> getOutNeighbors() const { return out_neighbors_; }
    void addInNeighbor(Node *neighbor) { in_neighbors_.push_back(neighbor); }
    void addOutNeighbor(Node *neighbor) { out_neighbors_.push_back(neighbor); }
    void calculateCentroid();
    Entity **getPosting() const { return posting_; }

private:
    int vector_num_;
    std::vector<Node*> out_neighbors_;
    std::vector<Node*> in_neighbors_;
    const int max_num_;
    const int dimension_;
    const size_t element_size_;
    void *centroid;
    Entity **posting_;
};

#endif // NOCONTROL_NODE_H
