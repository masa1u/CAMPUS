#ifndef CAMPUS_VERSION_H
#define CAMPUS_VERSION_H

#include "entity.h"
#include <vector>

class Version {
public:
    Version(int version, int max_size, int dimension, size_t element_size)
        : version_(version), max_size_(max_size), size_(0), dimension_(dimension), element_size_(element_size) {
        posting_ = new Entity*[max_size_];
    }

    ~Version() {
        for (int i = 0; i < size_; ++i) {
            delete posting_[i];
        }
        delete[] posting_;
    }

    int getVersion() const { return version_; }
    const void* getCentroid() const { return centroid; }
    void calculateCentroid();
    bool canAddVector() const { return size_ < max_size_; }
    void addVector(const void* vector, const int vector_id);
    std::vector<Entity*> getPosting() const {
        return std::vector<Entity*>(posting_, posting_ + size_);
    }

private:
    const int version_;
    const int max_size_;
    int size_;
    int dimension_;
    size_t element_size_;
    void *centroid;
    Entity **posting_;
};

#endif //CAMPUS_VERSION_H