#include "campus.h"
#include <cassert>
#include <iostream>


void CampusInsertExecutor::insert(){
RETRY:
    // If the campus is empty, create a new node and set it as the entry point
    if (campus_->getNodeNum() == 0) { 
        Node *new_node = new Node(campus_->getPositingLimit(),
            campus_->getDimension(), sizeof(float));
        if (!campus_->validationLock()) {
            goto RETRY;
        }
        if(!campus_->getNodeNum() == 0){
            campus_->validationUnlock();
            delete new_node;
            goto RETRY;
        }
        std::cout << "Inserting the first vector" << std::endl;
        campus_->setEntryPoint(new_node);
        campus_->incrementNodeNum();
        Version *latest_version = new_node->getLatestVersion();
        latest_version->addVector(insert_vector_, vector_id_);
        latest_version->calculateCentroid();
        campus_->validationUnlock();
        return;
    }else{
        // Find the nearest node to the insert_vector_
        Node *nearest_node = campus_->findExactNearestNode(insert_vector_, distance_);
        Version *latest_version = nearest_node->getLatestVersion();
        changed_versions_.push_back(latest_version);
        if (latest_version->canAddVector()) {
            // No need to split
            Version *new_version = new Version(latest_version->getVersion() + 1,  nearest_node,
                latest_version, campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
            new_version->copyPostingFromPrevVersion();
            new_version->copyNeighborFromPrevVersion();
            new_version->addVector(insert_vector_, vector_id_);
            new_versions_.push_back(new_version);
            std::cout << "just added a vector to the nearest node" << std::endl;
        }else{
            // Need to split
            splitCalculation(latest_version);
            std::cout << "Splitting the nearest node" << std::endl;
        }

        while(!campus_->validationLock()) {}
        if (validation()){
            if (new_nodes_.empty()) {
                campus_->setEntryPoint(nearest_node);
                campus_->validationUnlock();
                return;
            }else{
                // new_nodes_から1つランダムに選択し、entry_point_として設定
                int random_index = rand() % new_nodes_.size();
                campus_->setEntryPoint(new_nodes_[random_index]);
                campus_->validationUnlock();
                return;
            }
        }else{
            campus_->validationUnlock();
            std::cout << "Validation failed. Retry." << std::endl;
            abort();
            return;
        }
    }

}


void CampusInsertExecutor::splitCalculation(Version *spliting_version){
    Node *new_node1 = new Node(campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize());
    Node *new_node2 = new Node(campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize());
    // TODO: コンストラクタでprev_nodeを設定するように変更するか検討
    new_node1->setPrevNode(spliting_version->getNode());
    new_node2->setPrevNode(spliting_version->getNode());
    new_nodes_.push_back(new_node1);
    new_nodes_.push_back(new_node2);
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

   assignCalculation(new_node1, new_node2);
   connectNeighbors(spliting_version, new_node1, new_node2, campus_->getConnectionLimit());
   updateNeighbors(spliting_version, new_node1, new_node2, campus_->getConnectionLimit());
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
            if (existing_version->getNode() == neighbor_node->getLatestVersion()->getNode()) {
                new_version = existing_version;
                already_updated = true;
                break;
            }
        }
        if (!already_updated) {
            new_version = new Version(neighbor_node->getLatestVersion()->getVersion() + 1,
                neighbor_node->getLatestVersion()->getNode(), neighbor_node->getLatestVersion(),
                campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
            new_versions_.push_back(new_version);
            changed_versions_.push_back(neighbor_node->getLatestVersion());
            // TODO: copy関数の統合の検討
            new_version->copyNeighborFromPrevVersion();
            new_version->copyPostingFromPrevVersion();
            new_version->copyCentroidFromPrevVersion();
        }
        new_version->addInNeighbor(new_node1);
        new_version->addInNeighbor(new_node2);
    }
    neighbors1.push_back(new_node2);
    neighbors2.push_back(new_node1);

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
            assert(neighbor_version != nullptr);

            float distance = distance_->calculateDistance(new_node1->getLatestVersion()->getCentroid(),
                neighbor_version->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_version = neighbor_version;
            }
        }
        neighbors1.erase(std::remove_if(neighbors1.begin(), neighbors1.end(),
            [farthest_version](Node* node) { return node->getLatestVersion() == farthest_version; }), neighbors1.end());
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
            assert(neighbor_version != nullptr);

            float distance = distance_->calculateDistance(new_node2->getLatestVersion()->getCentroid(),
                neighbor_version->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_version = neighbor_version;
            }
        }
        neighbors2.erase(std::remove_if(neighbors2.begin(), neighbors2.end(),
            [farthest_version](Node* node) { return node->getLatestVersion() == farthest_version; }), neighbors2.end());
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
            new_version = new Version(neighbor_node->getLatestVersion()->getVersion() + 1,
                neighbor_node->getLatestVersion()->getNode(), neighbor_node->getLatestVersion(),
                campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
            new_versions_.push_back(new_version);
            changed_versions_.push_back(neighbor_node->getLatestVersion());
            // TODO: copy関数の統合の検討
            new_version->copyNeighborFromPrevVersion();
            new_version->copyPostingFromPrevVersion();
            new_version->copyCentroidFromPrevVersion();
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
            for (Node* neighbor_node : new_version->getOutNeighbors()) {
                // TODO: neighbor_nodeをvalidationするchanged_nodes_に含めるべきか
                // validationしなくても、NPA違反は起きない。グラフの接続の問題
                float distance = distance_->calculateDistance(new_version->getCentroid(),
                    neighbor_node->getLatestVersion()->getCentroid(), campus_->getDimension());
                if (distance > max_distance) {
                    max_distance = distance;
                    farthest_neighbor = neighbor_node;
                }
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
            if (!already_updated) {
                farthest_version = new Version(farthest_neighbor->getLatestVersion()->getVersion() + 1,
                    farthest_neighbor->getLatestVersion()->getNode(), farthest_neighbor->getLatestVersion(),
                    campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
                farthest_version->copyNeighborFromPrevVersion();
                farthest_version->copyPostingFromPrevVersion();
                farthest_version->copyCentroidFromPrevVersion();
                new_versions_.push_back(farthest_version);
                changed_versions_.push_back(farthest_neighbor->getLatestVersion());
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
                new_node1->getLatestVersion()->deleteVector(posting1[i]->id);
                if (closest_version->canAddVector()) {
                    closest_version->addVector(vector, posting1[i]->id);
                } else {
                    Version *spliting_version = closest_version;
                    splitCalculation(spliting_version);
                }
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
                new_node2->getLatestVersion()->deleteVector(posting2[i]->id);
                if (closest_version->canAddVector()) {
                    closest_version->addVector(vector, posting2[i]->id);
                } else {
                    Version *spliting_version = closest_version;
                    splitCalculation(spliting_version);
                }
            }
        }
    }

    std::vector<Node*> in_neighbors = spliting_version->getInNeighbors();
    for (Node* neighbor_node : in_neighbors){
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
            const void *old_centroid = neighbor_node->getLatestVersion()->getCentroid();
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
                    neighbor->deleteVector(posting[i]->id);
                    if (new_distance1 < new_distance2) {
                        if (new_node1->getLatestVersion()->canAddVector()) {
                            new_node1->getLatestVersion()->addVector(vector, posting[i]->id);
                        } else {
                            Version *spliting_version = new_node1->getLatestVersion();
                            new_nodes_.erase(std::remove(new_nodes_.begin(), new_nodes_.end(), new_node1), new_nodes_.end());
                            splitCalculation(spliting_version);
                        }
                    } else {
                        if (new_node2->getLatestVersion()->canAddVector()) {
                            new_node2->getLatestVersion()->addVector(vector, posting[i]->id);
                        } else {
                            Version *spliting_version = new_node2->getLatestVersion();
                            new_nodes_.erase(std::remove(new_nodes_.begin(), new_nodes_.end(), new_node1), new_nodes_.end());
                            splitCalculation(spliting_version);
                        }
                    }
                }
            }
        }
    }
}

bool CampusInsertExecutor::validation(){
    // Nodeがarchiveにされていたら並列リクエストによってsplitされている
    // Nodeのlatest_versionが自身のVersionの１つ前のVersionでない場合は、他のリクエストによって更新されている
    for (Version *version : changed_versions_) {
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
    int updater_id = campus_->getUpdateCounter();
    for (Node *node : new_nodes_) {
        node->getPrevNode()->setArchived();
        campus_->incrementNodeNum();
    }
    for (Version *version : new_versions_) {
        campus_->switchVersion(version->getNode(), version);
        version->setUpdaterId(updater_id);
    }
    for (Node *node : new_nodes_) {
        node->getLatestVersion()->setUpdaterId(updater_id);
    }
}

void CampusInsertExecutor::abort(){
    changed_versions_.clear();
    new_nodes_.clear();
    new_versions_.clear();
    insert();
}