#include "version.h"
#include <cstring>

void Version::calculateCentroid() {
    if (vector_num_ == 0) {
        std::memset(centroid, 0, dimension_ * element_size_);
        return;
    }

    std::memset(centroid, 0, dimension_ * element_size_);
    for (int i = 0; i < vector_num_; ++i) {
        const float* vec = static_cast<const float*>(posting_[i]->getVector());
        for (int j = 0; j < dimension_; ++j) {
            reinterpret_cast<float*>(centroid)[j] += vec[j];
        }
    }

    for (int j = 0; j < dimension_; ++j) {
        reinterpret_cast<float*>(centroid)[j] /= vector_num_;
    }
}

void Version::addVector(const void* vector, const int vector_id) {
    if (vector_num_ < max_num_) {
        posting_[vector_num_] = new Entity(vector_id, vector, dimension_, element_size_);
        vector_num_++;
    }
}

void Version::deleteVector(int vector_id) {
    for (int i = 0; i < vector_num_; ++i) {
        if (posting_[i]->id == vector_id) {
            // REVIEW: delete必要？
            delete posting_[i];
            for (int j = i; j < vector_num_ - 1; ++j) {
                posting_[j] = posting_[j + 1];
            }
            vector_num_--;
            break;
        }
    }
}

void Version::copyPostingFromPrevVersion() {
    if (prev_version_ != nullptr) {
        for (int i = 0; i < prev_version_->getVectorNum(); ++i) {
            addVector(prev_version_->getPosting()[i]->getVector(), prev_version_->getPosting()[i]->id);
        }
    }
}

void Version::copyNeighborFromPrevVersion() {
    if (prev_version_ != nullptr) {
        neighbors_ = prev_version_->getNeighbors();
    }
}