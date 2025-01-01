#include "campus.h"


void CampusInsertExecutor::insert(){
    // If the campus is empty, create a new node and set it as the entry point
    if (campus_->getNodeNum() == 0) { 
        Node *new_node = new Node(campus_->getPositingLimit(),
            campus_->getDimension(), sizeof(float));
        while (!campus_->validationLock()) {}
        campus_->addNodeToGraph(new_node);
        Version *latest_version = new_node->getLatestVersion();
        latest_version->addVector(insert_vector_, vector_id_);
        latest_version->calculateCentroid();
        campus_->validationUnlock();
        return;
    }else{
        // Find the nearest node to the insert_vector_
        Node *nearest_node = campus_->findExactNearestNode(insert_vector_, distance_);
        Version *latest_version = nearest_node->getLatestVersion();
        changed_nodes_.push_back(nearest_node);
        if (latest_version->canAddVector()) {
            // No need to split
            Version *new_version = new Version(latest_version->getVersion() + 1,  nearest_node,
                latest_version, campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
            new_version->copyPostingFromPrevVersion();
            new_version->copyNeighborFromPrevVersion();
            new_version->addVector(insert_vector_, vector_id_);
            new_versions_.push_back(new_version);
        }else{
            // Need to split
            splitCalculation(latest_version);
        }
    }

    if (validation()){
        return;
    }else{
        abort();
        return;
    }
}

void CampusInsertExecutor::splitCalculation(Version *spliting_version){
    std::vector<Node *> neighbors = spliting_version->getNeighbors();
    Node *new_node1 = new Node(campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize());
    Node *new_node2 = new Node(campus_->getPositingLimit(),
        campus_->getDimension(), campus_->getElementSize());
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
    // 元のノードのneighborsはどっちかのノードにだけつけるようにしよう
    std::vector<Node*> neighbors1 = spliting_version->getNeighbors();
    std::vector<Node*> neighbors2 = spliting_version->getNeighbors();
    neighbors1.push_back(new_node2);
    neighbors2.push_back(new_node1);

    // if the number of neighbors exceeds the connection limit, remove the farthest neighbor
    if (neighbors1.size() > connection_limit) {
        float max_distance = 0;
        Node* farthest_node = nullptr;
        for (Node* neighbor_node : neighbors1) {
            float distance = distance_->calculateDistance(new_node1->getLatestVersion()->getCentroid(),
                neighbor_node->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_node = neighbor_node;
            }
        }
        neighbors1.erase(std::remove(neighbors1.begin(), neighbors1.end(), farthest_node), neighbors1.end());
    }
    if (neighbors2.size() > connection_limit) {
        float max_distance = 0;
        Version* farthest_neighbor = nullptr;
        for (Version* neighbor_version : neighbors2) {
            Node *neighbor_node = campus_->getNode(neighbor_version->getNodeID());
            float distance = distance_->calculateDistance(new_node2->getLatestVersion()->getCentroid(),
                neighbor_node->getLatestVersion()->getCentroid(), campus_->getDimension());
            if (distance > max_distance) {
                max_distance = distance;
                farthest_neighbor = neighbor_version;
            }
        }
        neighbors2.erase(std::remove(neighbors2.begin(), neighbors2.end(), farthest_neighbor), neighbors2.end());
    }

    for (Version* neighbor_version : neighbors1) {
        Node *neighbor_node = campus_->getNode(neighbor_version->getNodeID());
        if (neighbor_node == nullptr) {
            continue;
        }
        neighbor_version->addNeighbor(new_node1->getLatestVersion());
        new_node1->getLatestVersion()->addNeighbor(neighbor_version);
    }
    for (Version* neighbor_version : neighbors2) {
        Node *neighbor_node = campus_->getNode(neighbor_version->getNodeID());
        if (neighbor_node == nullptr) {
            continue;
        }
        neighbor_version->addNeighbor(new_node2->getLatestVersion());
        new_node2->getLatestVersion()->addNeighbor(neighbor_version);
    }
}


void CampusInsertExecutor::updateNeighbors(Version *spliting_version, Node *new_node1, Node *new_node2, int connection_limit) {
    std::vector<Version*> updating_neighbors = spliting_version->getNeighbors();
    for (Version* neighbor_version : updating_neighbors) {
        // 既に更新があって、new_versions_に含まれている場合はそのVersionを取得、含まれていない場合は新しいVersionを作成
        bool already_updated = false;
        Version *new_version = nullptr;
        for (Version *existing_version : new_versions_) {
            if (existing_version->getNodeID() == neighbor_version->getNodeID()) {
                new_version = existing_version;
                already_updated = true;
                break;
            }
        }
        if (!already_updated) {
            Node *neighbor_node = campus_->getNode(neighbor_version->getNodeID());
            if (neighbor_node != nullptr) {
                new_version = new Version(neighbor_version->getNodeID(), neighbor_node->getLatestVersion()->getVersion() + 1,
                    neighbor_node->getLatestVersion(), campus_->getPositingLimit(), campus_->getDimension(), campus_->getElementSize());
                new_versions_.push_back(new_version);
                new_version->copyNeighborFromPrevVersion();
                new_version->copyPostingFromPrevVersion();
            }
        }

        new_version->addNeighbor(new_node1->getLatestVersion());
        new_version->addNeighbor(new_node2->getLatestVersion());
        new_version->deleteNeighbor(spliting_version);
        if (new_version->getNeighbors().size() > connection_limit) {
            float max_distance = 0;
            Version* farthest_neighbor = nullptr;
            for (Version* neighbor : new_version->getNeighbors()) {
                Node *neighbor_node = campus_->getNode(neighbor->getNodeID());
                float distance = distance_->calculateDistance(new_version->getCentroid(),
                    neighbor->getCentroid(), campus_->getDimension());
                if (distance > max_distance) {
                    max_distance = distance;
                    farthest_neighbor = neighbor;
                }
            }
            new_version->deleteNeighbor(farthest_neighbor);
        }
    }
}

void CampusInsertExecutor::reassignCalculation(Version *spliting_version, Node *new_node1, Node *new_node2) {
    std::vector<Version*> neighbors = spliting_version->getNeighbors();
    for (Version* neighbor_version : neighbors) {
        Node *neighbor = campus_->getNode(neighbor_version->getNodeID());
        if (neighbor == nullptr) {
            continue;
        }
        Entity **posting = neighbor->getLatestVersion()->getPosting();
        for (int i = 0; i < neighbor->getLatestVersion()->getSize(); ++i) {
            const void *vector = posting[i]->getVector();
            const void *old_centroid = neighbor->getLatestVersion()->getCentroid();
            const void *new_centroid1 = new_node1->getLatestVersion()->getCentroid();
            const void *new_centroid2 = new_node2->getLatestVersion()->getCentroid();
            float old_distance = distance_->calculateDistance(vector, old_centroid, campus_->getDimension());
            float new_distance1 = distance_->calculateDistance(vector, new_centroid1, campus_->getDimension());
            float new_distance2 = distance_->calculateDistance(vector, new_centroid2, campus_->getDimension());
            if (old_distance < new_distance1 && old_distance < new_distance2) {
                continue;
            } else {
                if (new_distance1 < new_distance2) {
                    new_node1->getLatestVersion()->addVector(vector, posting[i]->id);
                } else {
                    new_node2->getLatestVersion()->addVector(vector, posting[i]->id);
                }
            }
        }
    }
}

bool CampusInsertExecutor::validation(){
    campus_->validationLock();
    // Nodeがarchiveにされていたら並列リクエストによってsplitされている
    // Nodeのlatest_versionが自身のVersionの１つ前のVersionでない場合は、他のリクエストによって更新されている
    for (Version * version : changed_versions_){
        if (version->getNeighbors().size() > 2){
            campus_->validationUnlock();
            return false;
        }
    }

    campus_->validationUnlock();
}

void CampusInsertExecutor::abort(){
    changed_versions_.clear();
    new_nodes_.clear();
    new_versions_.clear();
    insert();
}