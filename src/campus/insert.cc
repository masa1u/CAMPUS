#include "campus.h"
#include <cassert>
#include <iostream>
#include <unordered_set>


void CampusInsertExecutor::insert(){
RETRY:
    // If the campus is empty, create a new node and set it as the entry point
    if (campus_->getNodeNum() == 0) { 
        Node *new_node = new Node(campus_->getPositingLimit(),
            campus_->getDimension(), sizeof(float));
        if (!campus_->validationLock()) {
            goto RETRY;
        }
        if(campus_->getNodeNum() != 0){
            campus_->validationUnlock();
            delete new_node;
            goto RETRY;
        }
        campus_->setEntryPoint(new_node);
        campus_->incrementNodeNum();
        campus_->addNode(new_node);
        Version *latest_version = new_node->getLatestVersion();
        latest_version->addVector(insert_vector_, vector_id_);
        latest_version->calculateCentroid();
        campus_->validationUnlock();
        return;
    } else {
        // Find the nearest node to the insert_vector_
        Node *nearest_node = campus_->findExactNearestNode(insert_vector_, distance_);
        if (nearest_node == nullptr) {
            goto RETRY;
        }
        Version *latest_version = nearest_node->getLatestVersion();
        changed_versions_.push_back(latest_version);
        if (latest_version->canAddVector()) {
            // No need to split
            Version *new_version = new Version(latest_version->getVersion() + 1,  nearest_node,
                latest_version, campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
            new_version->copyFromPrevVersion();
            new_version->addVector(insert_vector_, vector_id_);
            new_versions_.push_back(new_version);
        } else {
            // Need to split
            splitCalculation(latest_version, insert_vector_, vector_id_);
        }


        while(!campus_->validationLock()) {}
        if (validation()){
            int before_vector_num = campus_->countAllVectors();
            commit();
            int after_vector_num = campus_->countAllVectors();
            if (before_vector_num + 1 != after_vector_num) {
                std::cout << "before_vector_num: " << before_vector_num << " after_vector_num: " << after_vector_num << std::endl;
                assert(false);
            }
            if (new_nodes_.empty()) {
                campus_->setEntryPoint(nearest_node);
                campus_->validationUnlock();
                return;
            } else {
                // new_nodes_から1つランダムに選択し、entry_point_として設定
                int random_index = rand() % new_nodes_.size();
                campus_->setEntryPoint(new_nodes_[random_index]);
                campus_->validationUnlock();
                return;
            }
        } else {
            campus_->validationUnlock();
            abort();
            goto RETRY;
        }
    }

}


void CampusInsertExecutor::splitCalculation(Version *spliting_version, const void *insert_vector, int vector_id) {
    Node *new_node1 = new Node(campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize(), spliting_version->getNode());
    Node *new_node2 = new Node(campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize(), spliting_version->getNode());

    new_nodes_.push_back(new_node1);
    new_nodes_.push_back(new_node2);
    new_versions_.push_back(new_node1->getLatestVersion());
    new_versions_.push_back(new_node2->getLatestVersion());
    Entity **posting = spliting_version->getPosting();

    // randomly assign vectors to new nodes
    for (int i = 0; i < spliting_version->getVectorNum(); ++i) {
        const void *vector = posting[i]->getVector();
        int vector_id = posting[i]->id;
        if (i < spliting_version->getVectorNum() / 2) {
            new_node1->getLatestVersion()->addVector(vector, vector_id);
        }else{
            new_node2->getLatestVersion()->addVector(vector, vector_id);
        }
    }
    new_node1->getLatestVersion()->addVector(insert_vector, vector_id);

    assignCalculation(new_node1, new_node2);
    connectNeighbors(spliting_version, new_node1, new_node2, campus_->getConnectionLimit());
    updateNeighbors(spliting_version, new_node1, new_node2, campus_->getConnectionLimit());
    int before_vector_num = countBeforeAllVectors();
    int after_vector_num = countAfterAllVectors();
    reassignCalculation(spliting_version, new_node1, new_node2);
}

void CampusInsertExecutor::assignCalculation(Node *new_node1, Node *new_node2) {
    // k-means clustering for the new nodes
    while (true) {
        new_node1->getLatestVersion()->calculateCentroid();
        new_node2->getLatestVersion()->calculateCentroid();
        bool changed = false;
        for (int i = 0; i < new_node1->getLatestVersion()->getVectorNum(); ++i) {
            const void *vector = new_node1->getLatestVersion()->getPosting()[i]->getVector();
            int vector_id = new_node1->getLatestVersion()->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getLatestVersion()->getCentroid(), campus_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance1 > distance2) {
                new_node1->getLatestVersion()->deleteVector(vector_id);
                new_node2->getLatestVersion()->addVector(vector, vector_id);
                changed = true;
                i--;
            }
        }
        for (int i = 0; i < new_node2->getLatestVersion()->getVectorNum(); ++i) {
            const void *vector = new_node2->getLatestVersion()->getPosting()[i]->getVector();
            int vector_id = new_node2->getLatestVersion()->getPosting()[i]->id;
            float distance1 = distance_->calculateDistance(vector,
                new_node1->getLatestVersion()->getCentroid(), campus_->getDimension());
            float distance2 = distance_->calculateDistance(vector,
                new_node2->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance1 < distance2) {
                new_node2->getLatestVersion()->deleteVector(vector_id);
                new_node1->getLatestVersion()->addVector(vector, vector_id);
                changed = true;
                i--;
            }
        }
        if (!changed) {
            break;
        }
    }
}



void CampusInsertExecutor::connectNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit) {
    std::vector<Node*> neighbors1 = spliting_version->getOutNeighbors();
    std::vector<Node*> neighbors2 = spliting_version->getOutNeighbors();
    // neighbors1がchanged_nodes_に含まれている場合は、new_versions_に含まれているVersionを取得
    // 含まれていない場合は、新しいVersionを作成し、new_versions_に追加
    for (Node* neighbor_node : neighbors1) {
        bool already_updated = false;
        Version *new_version = nullptr;
        for (Version *existing_version : new_versions_) {
            if (existing_version->getNode() == neighbor_node) {
                new_version = existing_version;
                already_updated = true;
                break;
            }
        }
        if (!already_updated) {
            Version *changed_version = neighbor_node->getLatestVersion();
            new_version = new Version(changed_version->getVersion() + 1,
                changed_version->getNode(), changed_version,
                campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());;

            new_version->copyFromPrevVersion();
            new_versions_.push_back(new_version);
            changed_versions_.push_back(changed_version);
        }
        new_version->addInNeighbor(new_node1);
        new_version->addInNeighbor(new_node2);
        new_version->deleteInNeighbor(spliting_version->getNode());
    }
    neighbors1.push_back(new_node2);
    neighbors2.push_back(new_node1);
    new_node1->getLatestVersion()->addInNeighbor(new_node2);
    new_node2->getLatestVersion()->addInNeighbor(new_node1);

    // if the number of neighbors exceeds the connection limit, remove the farthest neighbor
    if (neighbors1.size() > connection_limit) {
        float max_distance = 0;
        Version* farthest_version = nullptr;
        for (Node* neighbor_node : neighbors1) {
            Version *neighbor_version = nullptr;
            for (Version *existing_version : new_versions_) {
                if (existing_version->getNode() == neighbor_node) {
                    neighbor_version = existing_version;
                    break;
                }
            }
            if (neighbor_node==new_node2){
                neighbor_version = new_node2->getLatestVersion();
            }
            assert(neighbor_version != nullptr);

            float distance = distance_->calculateDistance(new_node1->getLatestVersion()->getCentroid(),
                neighbor_version->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_version = neighbor_version;
            }
        }
        // TODO: 書き変わっている時の処理
        neighbors1.erase(std::remove_if(neighbors1.begin(), neighbors1.end(),
            [farthest_version](Node* node) {
                return node == farthest_version->getNode();
            }), neighbors1.end());
        farthest_version->deleteInNeighbor(new_node1);
    }
    if (neighbors2.size() > connection_limit) {
        float max_distance = 0;
        Version* farthest_version = nullptr;
        for (Node* neighbor_node : neighbors2) {
            Version *neighbor_version = nullptr;
            for (Version *existing_version : new_versions_) {
                if (existing_version->getNode() == neighbor_node) {
                    neighbor_version = existing_version;
                    break;
                }
            }
            if (neighbor_node==new_node1){
                neighbor_version = new_node1->getLatestVersion();
            }
            assert(neighbor_version != nullptr);

            float distance = distance_->calculateDistance(new_node2->getLatestVersion()->getCentroid(),
                neighbor_version->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_version = neighbor_version;
            }
        }
        neighbors2.erase(std::remove_if(neighbors2.begin(), neighbors2.end(),
            [farthest_version](Node* node) {
                return node == farthest_version->getNode();
            }), neighbors2.end());
        farthest_version->deleteInNeighbor(new_node2);
    }

    // neighbor1とneighbor2をそれぞれ、new_node1とnew_node2のout_neighbors_に設定
    for (Node* neighbor_node : neighbors1) {
        new_node1->getLatestVersion()->addOutNeighbor(neighbor_node);
    }
    for (Node* neighbor_node : neighbors2) {
        new_node2->getLatestVersion()->addOutNeighbor(neighbor_node);
    }
}



void CampusInsertExecutor::updateNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit) {
    std::vector<Node*> updating_neighbors = spliting_version->getInNeighbors();
    for (Node* neighbor_node : updating_neighbors) {
        bool already_updated = false;
        Version *new_version = nullptr;
        for (Version *existing_version : new_versions_) {
            if (existing_version->getNode() == neighbor_node->getLatestVersion()->getNode()) {
                new_version = existing_version;
                already_updated = true;
                break;
            }
        }
        if (!already_updated) {
            Version* changed_version = neighbor_node->getLatestVersion();
            new_version = new Version(changed_version->getVersion() + 1,
                changed_version->getNode(), changed_version,
                campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());

            new_version->copyFromPrevVersion();
            new_versions_.push_back(new_version);
            changed_versions_.push_back(changed_version);
        }
    }

    for (Node* neighbor_node : updating_neighbors) {
        Version *new_version = nullptr;
        for (Version *existing_version : new_versions_) {
            if (existing_version->getNode() == neighbor_node) {
                new_version = existing_version;
                break;
            }
        }
        assert(new_version != nullptr);

        new_version->addOutNeighbor(new_node1);
        new_version->addOutNeighbor(new_node2);
        new_node1->getLatestVersion()->addInNeighbor(neighbor_node);
        new_node2->getLatestVersion()->addInNeighbor(neighbor_node);

        new_version->deleteOutNeighbor(spliting_version->getNode());

        if (new_version->getOutNeighbors().size() > connection_limit) {
            float max_distance = 0;
            Node* farthest_neighbor = nullptr;
            for (Node* neighbor_neighbor : new_version->getOutNeighbors()) {
                // TODO: getLatestVersion()を使うべきかどうか
                float distance = distance_->calculateDistance(new_version->getCentroid(),
                    neighbor_neighbor->getLatestVersion()->getCentroid(), campus_->getDimension());
                if (distance > max_distance) {
                    max_distance = distance;
                    farthest_neighbor = neighbor_neighbor;
                }
                assert(distance != 0);
            }
            new_version->deleteOutNeighbor(farthest_neighbor);
            // TODO: farthest_neighborをreadするタイミングについて検討
            Version *farthest_version = nullptr;
            bool already_updated = false;
            for (Version *existing_version : new_versions_) {
                if (existing_version->getNode() == farthest_neighbor) {
                    farthest_version = existing_version;
                    already_updated = true;
                    break;
                }
            }
            if (farthest_neighbor==new_node1){
                farthest_version = new_node1->getLatestVersion();
                already_updated = true;
            }
            if (farthest_neighbor==new_node2){
                farthest_version = new_node2->getLatestVersion();
                already_updated = true;
            }
            if (!already_updated) {
                Version* changed_version = farthest_neighbor->getLatestVersion();
                farthest_version = new Version(changed_version->getVersion() + 1,
                    changed_version->getNode(), changed_version,
                    campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
                farthest_version->copyFromPrevVersion();
                new_versions_.push_back(farthest_version);
                changed_versions_.push_back(changed_version);
            }
            farthest_version->deleteInNeighbor(new_version->getNode());
        }

    }

}

void CampusInsertExecutor::reassignCalculation(Version *spliting_version, Node *new_node1, Node *new_node2) {
    Entity **posting1 = new_node1->getLatestVersion()->getPosting();
    for (int i = 0; i < new_node1->getLatestVersion()->getVectorNum(); ++i) {
        const void *vector = posting1[i]->getVector();
        const void *old_centroid = spliting_version->getCentroid();
        const void *new_centroid1 = new_node1->getLatestVersion()->getCentroid();
        float old_distance = distance_->calculateDistance(vector, old_centroid, campus_->getDimension());
        float new_distance1 = distance_->calculateDistance(vector, new_centroid1, campus_->getDimension());
        if (old_distance > new_distance1) {
            continue;
        } else {
            float min_distance = new_distance1;
            Version *closest_version = new_node1->getLatestVersion();
            for (Node *neighbor_node : new_node1->getLatestVersion()->getOutNeighbors()) {
                if (neighbor_node == new_node2) {
                    // assignでnew_node2より近いことは確定
                    continue;
                }
                Version *neighbor = nullptr;
                for (Version *existing_version : new_versions_) {
                    if (existing_version->getNode() == neighbor_node) {
                        neighbor = existing_version;
                        break;
                    }
                }
                assert(neighbor != nullptr);

                float neighbor_distance = distance_->calculateDistance(vector,
                    neighbor->getCentroid(), campus_->getDimension());
                if (neighbor_distance < min_distance) {
                    min_distance = neighbor_distance;
                    closest_version = neighbor;
                }
            }
            if (closest_version == new_node1->getLatestVersion()) {
                continue;
            } else {
                int vector_id = posting1[i]->id;
                new_node1->getLatestVersion()->deleteVector(vector_id);
                assert(closest_version != new_node2->getLatestVersion());
                if (closest_version->canAddVector()) {
                    closest_version->addVector(vector, vector_id);
                } else {
                    Version *split_version = closest_version;
                    // delete split_version from new_versions_
                    new_versions_.erase(std::remove(new_versions_.begin(), new_versions_.end(),
                        split_version), new_versions_.end());
                    splitCalculation(split_version, vector, vector_id);
                }
                i--;
            }
        }
    }


    Entity **posting2 = new_node2->getLatestVersion()->getPosting();
    for (int i = 0; i < new_node2->getLatestVersion()->getVectorNum(); ++i) {
        const void *vector = posting2[i]->getVector();
        const void *old_centroid = spliting_version->getCentroid();
        const void *new_centroid2 = new_node2->getLatestVersion()->getCentroid();
        float old_distance = distance_->calculateDistance(vector, old_centroid, campus_->getDimension());
        float new_distance2 = distance_->calculateDistance(vector, new_centroid2, campus_->getDimension());
        if (old_distance > new_distance2) {
            continue;
        } else {
            float min_distance = new_distance2;
            Version *closest_version = new_node2->getLatestVersion();
            for (Node *neighbor_node : new_node2->getLatestVersion()->getOutNeighbors()) {
                if (neighbor_node == new_node1) {
                    continue;
                }
                Version *neighbor = nullptr;
                for (Version *existing_version : new_versions_) {
                    if (existing_version->getNode() == neighbor_node) {
                        neighbor = existing_version;
                        break;
                    }
                }
                assert(neighbor != nullptr);

                float neighbor_distance = distance_->calculateDistance(vector,
                    neighbor->getCentroid(), campus_->getDimension());
                if (neighbor_distance < min_distance) {
                    min_distance = neighbor_distance;
                    closest_version = neighbor;
                }
            }
            if (closest_version == new_node2->getLatestVersion()) {
                continue;
            } else {
                int vector_id = posting2[i]->id;
                new_node2->getLatestVersion()->deleteVector(vector_id);
                assert(closest_version != new_node1->getLatestVersion());
                if (closest_version->canAddVector()) {
                    closest_version->addVector(vector, vector_id);
                } else {
                    Version *split_version = closest_version;
                    new_versions_.erase(std::remove(new_versions_.begin(), new_versions_.end(),
                        split_version), new_versions_.end());
                    splitCalculation(split_version, vector, vector_id);
                }
                i--;
            }
        }
    }

    // new_node1とnew_node2のin_neighborsの集合を取得してstd::vectorに変換
    std::unordered_set<Node*> in_neighbors_set;
    for (Node* neighbor_node : new_node1->getLatestVersion()->getInNeighbors()) {
        in_neighbors_set.insert(neighbor_node);
    }
    for (Node* neighbor_node : new_node2->getLatestVersion()->getInNeighbors()) {
        in_neighbors_set.insert(neighbor_node);
    }
    std::vector<Node*> in_neighbors(in_neighbors_set.begin(), in_neighbors_set.end());

    for (Node* neighbor_node : in_neighbors){
        if (neighbor_node == new_node1 || neighbor_node == new_node2) {
            continue;
        }
        Version *neighbor = nullptr;
        for (Version *existing_version : new_versions_) {
            if (existing_version->getNode() == neighbor_node) {
                neighbor = existing_version;
                break;
            }
        }
        assert(neighbor != nullptr);

        Entity **posting = neighbor->getPosting();
        for (int i = 0; i < neighbor->getVectorNum(); ++i) {
            const void *vector = posting[i]->getVector();
            const void *old_centroid = spliting_version->getCentroid();
            const void *new_centroid1 = new_node1->getLatestVersion()->getCentroid();
            const void *new_centroid2 = new_node2->getLatestVersion()->getCentroid();
            float old_distance = distance_->calculateDistance(vector, old_centroid, campus_->getDimension());
            float new_distance1 = distance_->calculateDistance(vector, new_centroid1, campus_->getDimension());
            float new_distance2 = distance_->calculateDistance(vector, new_centroid2, campus_->getDimension());
            if (old_distance < new_distance1 && old_distance < new_distance2) {
                continue;
            } else {
                float current_distance = distance_->calculateDistance(vector, neighbor->getCentroid(), campus_->getDimension());
                if (current_distance < new_distance1 && current_distance < new_distance2) {
                    continue;
                } else {
                    int vector_id = posting[i]->id;
                    neighbor->deleteVector(vector_id);
                    if (new_distance1 < new_distance2) {
                        if (new_node1->getLatestVersion()->canAddVector()) {
                            new_node1->getLatestVersion()->addVector(vector, vector_id);
                        } else {
                            Version *split_version = new_node1->getLatestVersion();
                            // new_nodesから削除ということは、前のノードをarchiveできない？
                            // split→splitのprevious nodeを何に設定するか
                            // new_nodes_.erase(std::remove(new_nodes_.begin(), new_nodes_.end(), new_node1), new_nodes_.end());
                            new_versions_.erase(std::remove(new_versions_.begin(), new_versions_.end(), new_node1->getLatestVersion()), new_versions_.end());
                            // if (countAfterAllVectors()+10 != countBeforeAllVectors()) {
                            //     std::cout << "Error: countAfterAllVectors() != countBeforeAllVectors() + 1" << std::endl;
                            //     std::cout << countBeforeAllVectors() << " " << countAfterAllVectors() << std::endl;
                            //     assert(false);
                            // }
                            splitCalculation(split_version, vector, vector_id);
                        }
                    } else {
                        if (new_node2->getLatestVersion()->canAddVector()) {
                            new_node2->getLatestVersion()->addVector(vector, vector_id);
                        } else {
                            Version *split_version = new_node2->getLatestVersion();
                            // new_nodes_.erase(std::remove(new_nodes_.begin(), new_nodes_.end(), new_node1), new_nodes_.end());
                            new_versions_.erase(std::remove(new_versions_.begin(), new_versions_.end(), new_node2->getLatestVersion()), new_versions_.end());
                            // if (countAfterAllVectors()+10 != countBeforeAllVectors() ) {
                            //     std::cout << "Error: countAfterAllVectors() != countBeforeAllVectors() + 1" << std::endl;
                            //     std::cout << countBeforeAllVectors() << " " << countAfterAllVectors() << std::endl;
                            //     assert(false);
                            // }
                            splitCalculation(split_version, vector, vector_id);
                        }
                    }
                    i--;
                }
            }
        }
    }
}

bool CampusInsertExecutor::validation(){
    // Nodeがarchiveにされていたら並列リクエストによってsplitされている
    // Nodeのlatest_versionが自身のVersionの１つ前のVersionでない場合は、他のリクエストによって更新されている
    for (Version *version : changed_versions_) {
        assert(version != nullptr);
        assert(version->getNode() != nullptr);
        if (version->getNode()->isArchived()) {
            return false;
        }
        if (version->getNode()->getLatestVersion() != version) {
            return false;
        }
    }
    return true;
}

void CampusInsertExecutor::commit(){
    // if (countAfterAllVectors() != countBeforeAllVectors() + 1) {
    //     std::cout << "Error: countAfterAllVectors() != countBeforeAllVectors() + 1" << std::endl;
    //     std::cout << countBeforeAllVectors() << " " << countAfterAllVectors() << std::endl;
    //     assert(false);
    // }
    campus_->incrementUpdateCounter();
    int updater_id = campus_->getUpdateCounter();
    for (Version *version : new_versions_) {
        campus_->switchVersion(version->getNode(), version);
        version->setUpdaterId(updater_id);
    }

    for (Node *node : new_nodes_) {
        if (node->getPrevNode() != nullptr) {
            node->getPrevNode()->setArchived();
        }
        assert(node != nullptr);
        campus_->addNode(node);
        node->getLatestVersion()->setUpdaterId(updater_id);
        // cascade splitによりarchiveにされてる可能性あるから単純なincrementはできない
        campus_->incrementNodeNum();
    }
}

void CampusInsertExecutor::abort(){
    changed_versions_.clear();
    new_nodes_.clear();
    new_versions_.clear();
}