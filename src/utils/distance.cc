#include "distance.h"
#include <cmath>
#include <cstring>

Distance::Distance() {}

L2Distance::L2Distance() {}

float L2Distance::calculateDistance(const void *vector1, const void *vector2, size_t dimension) {
    const float *pVect1 = static_cast<const float*>(vector1);
    const float *pVect2 = static_cast<const float*>(vector2);

    float res = 0;
    for (size_t i = 0; i < dimension; i++) {
        float diff = pVect1[i] - pVect2[i];
        res += diff * diff;
    }
    return res;
}

AngularDistance::AngularDistance() {}

float AngularDistance::calculateDistance(const void *vector1, const void *vector2, size_t dimension) {
    const float *pVect1 = static_cast<const float*>(vector1);
    const float *pVect2 = static_cast<const float*>(vector2);

    float dot_product = 0.0;
    float norm1 = 0.0;
    float norm2 = 0.0;
    for (size_t i = 0; i < dimension; i++) {
        dot_product += pVect1[i] * pVect2[i];
        norm1 += pVect1[i] * pVect1[i];
        norm2 += pVect2[i] * pVect2[i];
    }
    return std::acos(dot_product / (std::sqrt(norm1) * std::sqrt(norm2)));
}