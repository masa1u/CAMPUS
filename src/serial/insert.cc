#include "serial.h"
#include <cassert>
#include <iostream>
#include <unordered_set>


void SerialInsertExecutor::insert(){
    while (!serial_->insertLock()) {}
    // If the campus is empty, create a new node and set it as the entry point
    if (serial_->getNodeNum() == 0){
        Node *new_node = new Node(serial_->getPositingLimit(),
            serial_->getDimension(), sizeof(float));
        serial_->setEntryPoint(new_node);
        serial_->addNode(new_node);
        serial_->incrementNodeNum();
        new_node->addVector(insert_vector_, vector_id_);
    } else {
        // Find the nearest node to the insert_vector_
        Node *nearest_node = serial_->findExactNearestNode(insert_vector_, distance_);
        if (nearest_node->canAddVector()) {
            nearest_node->addVector(insert_vector_, vector_id_);
        } else {
            // Need to split
            split(nearest_node, insert_vector_, vector_id_);
        }
    }

    serial_->insertUnlock();
    return;
}


void SerialInsertExecutor::split(Node *spliting_node, const void *insert_vector, int vector_id) {
    Node *new_node1 = new Node(serial_->getPositingLimit(),
        serial_->getDimension(), serial_->getElementSize());
    Node *new_node2 = new Node(serial_->getPositingLimit(),
        serial_->getDimension(), serial_->getElementSize());
    serial_->incrementNodeNum();
    serial_->addNode(new_node1);
    serial_->addNode(new_node2);
    serial_->deleteNode(spliting_node);

    Entity **posting = spliting_node->getPosting();
    // randomly assign vectors to new nodes
    for (int i = 0; i < spliting_node->getVectorNum(); ++i) {
        const void *vector = posting[i]->getVector();
        int vector_id = posting[i]->id;
        if (i < spliting_node->getVectorNum() / 2) {
            new_node1->addVector(vector, vector_id);
        }else{
            new_node2->addVector(vector, vector_id);
        }
    }
    new_node1->addVector(insert_vector, vector_id);

    assignCalculation(new_node1, new_node2);
    connectNeighbors(spliting_node, new_node1, new_node2, serial_->getConnectionLimit());
    updateNeighbors(spliting_node, new_node1, new_node2, serial_->getConnectionLimit());
    reassignCalculation(spliting_node, new_node1, new_node2);
}

void SerialInsertExecutor::assignCalculation(Node *new_node1, Node *new_node2) {
    // k-means clustering for the new nodes
    while (true) {
        new_node1->calculateCentroid();
        new_node2->calculateCentroid();
        bool changed = false;
        for (int i = 0; i < new_node1->getVectorNum(); ++i) {
            const void *vector = new_node1->getPosting()[i]->getVector();
            int vector_id = new_node1->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getCentroid(), serial_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getCentroid(), serial_->getDimension());
            if (distance1 > distance2) {
                new_node1->deleteVector(vector_id);
                new_node2->addVector(vector, vector_id);
                changed = true;
                i--;
            }
        }
        for (int i = 0; i < new_node2->getVectorNum(); ++i) {
            const void *vector = new_node2->getPosting()[i]->getVector();
            int vector_id = new_node2->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getCentroid(), serial_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getCentroid(), serial_->getDimension());
            if (distance1 < distance2) {
                new_node2->deleteVector(vector_id);
                new_node1->addVector(vector, vector_id);
                changed = true;
                i--;
            }
        }
        if (!changed) {
            break;
        }
    }
}



void SerialInsertExecutor::connectNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit) {
    std::vector<Node*> neighbors1 = spliting_node->getOutNeighbors();
    std::vector<Node*> neighbors2 = spliting_node->getOutNeighbors();

    for (Node* neighbor_node : neighbors1) {
        neighbor_node->deleteInNeighbor(spliting_node);
    }

    neighbors1.push_back(new_node2);
    neighbors2.push_back(new_node1);

    // if the number of neighbors exceeds the connection limit, remove the farthest neighbor
    if (neighbors1.size() > connection_limit) {
        float max_distance = 0;
        Node* farthest_neighbor = nullptr;
        for (Node* neighbor_node : neighbors1) {
            float distance = distance_->calculateDistance(new_node1->getCentroid(),
                neighbor_node->getCentroid(), serial_->getDimension());

            if (distance > max_distance) {
                max_distance = distance;
                farthest_neighbor = neighbor_node;
            }
        }

        neighbors1.erase(std::remove_if(neighbors1.begin(), neighbors1.end(),
            [farthest_neighbor](Node* node) {
                return node == farthest_neighbor;
            }), neighbors1.end());
    }
    if (neighbors2.size() > connection_limit) {
        float max_distance = 0;
        Node* farthest_neighbor = nullptr;
        for (Node* neighbor_node : neighbors2) {
            float distance = distance_->calculateDistance(new_node2->getCentroid(),
                neighbor_node->getCentroid(), serial_->getDimension());

            if (distance > max_distance) {
                max_distance = distance;
                farthest_neighbor = neighbor_node;
            }
        }

        neighbors2.erase(std::remove_if(neighbors2.begin(), neighbors2.end(),
            [farthest_neighbor](Node* node) {
                return node == farthest_neighbor;
            }), neighbors2.end());
    }

    // neighbor1とneighbor2をそれぞれ、new_node1とnew_node2のout_neighbors_に設定
    for (Node* neighbor_node : neighbors1) {
        new_node1->addOutNeighbor(neighbor_node);
        neighbor_node->addInNeighbor(new_node1);
    }
    for (Node* neighbor_node : neighbors2) {
        new_node2->addOutNeighbor(neighbor_node);
        neighbor_node->addInNeighbor(new_node2);
    }
}



void SerialInsertExecutor::updateNeighbors(Node *spliting_node, Node *new_node1, Node *new_node2, int connection_limit) {
    std::vector<Node*> updating_neighbors = spliting_node->getInNeighbors();

    for (Node* neighbor_node : updating_neighbors) {
        neighbor_node->addOutNeighbor(new_node1);
        neighbor_node->addOutNeighbor(new_node2);
        new_node1->addInNeighbor(neighbor_node);
        new_node2->addInNeighbor(neighbor_node);

        neighbor_node->deleteOutNeighbor(spliting_node);

        if (neighbor_node->getOutNeighbors().size() > connection_limit) {
            float max_distance = 0;
            Node* farthest_neighbor = nullptr;
            for (Node* neighbor_neighbor : neighbor_node->getOutNeighbors()) {
                float distance = distance_->calculateDistance(neighbor_node->getCentroid(),
                    neighbor_neighbor->getCentroid(), serial_->getDimension());
                if (distance > max_distance) {
                    max_distance = distance;
                    farthest_neighbor = neighbor_neighbor;
                }
                assert(distance != 0);
            }
            neighbor_node->deleteOutNeighbor(farthest_neighbor);
            farthest_neighbor->deleteInNeighbor(neighbor_node);
        }
    }

}

void SerialInsertExecutor::reassignCalculation(Node *spliting_node, Node *new_node1, Node *new_node2) {
    Entity **posting1 = new_node1->getPosting();
    for (int i = 0; i < new_node1->getVectorNum(); ++i) {
        const void *vector = posting1[i]->getVector();
        const void *old_centroid = spliting_node->getCentroid();
        const void *new_centroid1 = new_node1->getCentroid();
        float old_distance = distance_->calculateDistance(vector, old_centroid, serial_->getDimension());
        float new_distance1 = distance_->calculateDistance(vector, new_centroid1, serial_->getDimension());
        if (old_distance > new_distance1) {
            continue;
        } else {
            float min_distance = new_distance1;
            Node *closest_node = new_node1;
            for (Node *neighbor_node : new_node1->getOutNeighbors()) {
                if (neighbor_node == new_node2) {
                    // assignでnew_node2より近いことは確定
                    continue;
                }

                float neighbor_distance = distance_->calculateDistance(vector,
                    neighbor_node->getCentroid(), serial_->getDimension());
                if (neighbor_distance < min_distance) {
                    min_distance = neighbor_distance;
                    closest_node = neighbor_node;
                }
            }
            if (closest_node == new_node1) {
                continue;
            } else {
                int vector_id = posting1[i]->id;
                new_node1->deleteVector(vector_id);
                if (closest_node->canAddVector()) {
                    closest_node->addVector(vector, vector_id);
                } else {
                    split(closest_node, vector, vector_id);
                }
                i--;
            }
        }
    }


    Entity **posting2 = new_node2->getPosting();
    for (int i = 0; i < new_node2->getVectorNum(); ++i) {
        const void *vector = posting2[i]->getVector();
        const void *old_centroid = spliting_node->getCentroid();
        const void *new_centroid2 = new_node2->getCentroid();
        float old_distance = distance_->calculateDistance(vector, old_centroid, serial_->getDimension());
        float new_distance2 = distance_->calculateDistance(vector, new_centroid2, serial_->getDimension());
        if (old_distance > new_distance2) {
            continue;
        } else {
            float min_distance = new_distance2;
            Node *closest_node = new_node2;
            for (Node *neighbor_node : new_node2->getOutNeighbors()) {
                if (neighbor_node == new_node1) {
                    // assignでnew_node2より近いことは確定
                    continue;
                }

                float neighbor_distance = distance_->calculateDistance(vector,
                    neighbor_node->getCentroid(), serial_->getDimension());
                if (neighbor_distance < min_distance) {
                    min_distance = neighbor_distance;
                    closest_node = neighbor_node;
                }
            }
            if (closest_node == new_node2) {
                continue;
            } else {
                int vector_id = posting2[i]->id;
                new_node2->deleteVector(vector_id);
                if (closest_node->canAddVector()) {
                    closest_node->addVector(vector, vector_id);
                } else {
                    split(closest_node, vector, vector_id);
                }
                i--;
            }
        }
    }


    std::unordered_set<Node*> in_neighbors_set;
    for (Node *neighbor_node : new_node1->getInNeighbors()) {
        in_neighbors_set.insert(neighbor_node);
    }
    for (Node *neighbor_node : new_node2->getInNeighbors()) {
        in_neighbors_set.insert(neighbor_node);
    }
    std::vector<Node*> in_neighbors(in_neighbors_set.begin(), in_neighbors_set.end());

    for (Node* neighbor_node : in_neighbors){
        if (neighbor_node == new_node1 || neighbor_node == new_node2) {
            continue;
        }
        Entity **posting = neighbor_node->getPosting();
        for (int i = 0; i < neighbor_node->getVectorNum(); ++i) {
            const void *vector = posting[i]->getVector();
            const void *old_centroid = neighbor_node->getCentroid();
            const void *new_centroid1 = new_node1->getCentroid();
            const void *new_centroid2 = new_node2->getCentroid();
            float old_distance = distance_->calculateDistance(vector, old_centroid, serial_->getDimension());
            float new_distance1 = distance_->calculateDistance(vector, new_centroid1, serial_->getDimension());
            float new_distance2 = distance_->calculateDistance(vector, new_centroid2, serial_->getDimension());
            if (old_distance < new_distance1 && old_distance < new_distance2) {
                continue;
            } else {
                float current_distance = distance_->calculateDistance(vector, neighbor_node->getCentroid(), serial_->getDimension());
                if (current_distance < new_distance1 && current_distance < new_distance2) {
                    continue;
                } else {
                    int vector_id = posting[i]->id;
                    neighbor_node->deleteVector(vector_id);
                    if (new_distance1 < new_distance2) {
                        if (new_node1->canAddVector()) {
                            new_node1->addVector(vector, vector_id);
                        } else {
                            split(new_node1, vector, vector_id);
                        }
                    } else {
                        if (new_node2->canAddVector()) {
                            new_node2->addVector(vector, vector_id);
                        } else {
                            split(new_node2, vector, vector_id);
                        }
                    }
                    // 削除した時はpostingのindexがずれるので、iをデクリメント
                    i--;
                }
            }
        }
    }

}
