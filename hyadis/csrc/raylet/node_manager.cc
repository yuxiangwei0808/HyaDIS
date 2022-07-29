#include "hyadis/csrc/raylet/node_manager.h"

namespace ray {

namespace raylet {

HyadisNodeManager::HyadisNodeManager(
    instrumented_io_context &io_service, const NodeID &self_node_id,
    const std::string &self_node_name, const NodeManagerConfig &config,
    const ObjectManagerConfig &object_manager_config,
    std::shared_ptr<gcs::GcsClient> gcs_client,
    std::shared_ptr<scheduling::SchedulerClient> scheduler_client)
    : NodeManager(io_service, self_node_id, self_node_name, config,
                  object_manager_config, gcs_client),
      scheduler_client_(scheduler_client) {
  scheduler_client_->Hello();
}

void HyadisNodeManager::ProcessNewClient(ray::ClientConnection &client) {
  scheduler_client_->Hello();
  NodeManager::ProcessNewClient(client);
}

void HyadisNodeManager::HandleUpdateResourceUsage(
    const rpc::UpdateResourceUsageRequest &request,
    rpc::UpdateResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  scheduler_client_->Hello();
  NodeManager::HandleUpdateResourceUsage(request, reply, send_reply_callback);
}

void HyadisNodeManager::HandleRequestWorkerLease(
    const rpc::RequestWorkerLeaseRequest &request,
    rpc::RequestWorkerLeaseReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  scheduler_client_->Hello();
  NodeManager::HandleRequestWorkerLease(request, reply, send_reply_callback);
}

}  // namespace raylet
}  // namespace ray
