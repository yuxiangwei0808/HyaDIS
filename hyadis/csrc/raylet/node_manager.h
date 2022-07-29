#pragma once

#include "hyadis/csrc/scheduler/scheduler.h"
#include "hyadis/csrc/thirdparty/ray/raylet/node_manager.h"

namespace ray {

namespace raylet {

class HyadisNodeManager : public NodeManager {
 public:
  HyadisNodeManager(
      instrumented_io_context &io_service, const NodeID &self_node_id,
      const std::string &self_node_name, const NodeManagerConfig &config,
      const ObjectManagerConfig &object_manager_config,
      std::shared_ptr<gcs::GcsClient> gcs_client,
      std::shared_ptr<scheduling::SchedulerClient> scheduler_client);

  /// Process a new client connection.
  ///
  /// \param client The client to process.
  /// \return Void.
  void ProcessNewClient(ray::ClientConnection &client);

 private:
  void HandleUpdateResourceUsage(
      const rpc::UpdateResourceUsageRequest &request,
      rpc::UpdateResourceUsageReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRequestWorkerLease(
      const rpc::RequestWorkerLeaseRequest &request,
      rpc::RequestWorkerLeaseReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  std::shared_ptr<scheduling::SchedulerClient> scheduler_client_;
};

}  // namespace raylet

}  // namespace ray
