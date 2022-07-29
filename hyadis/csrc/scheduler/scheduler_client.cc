#include "absl/strings/str_split.h"
#include "hyadis/csrc/scheduler/scheduler.h"
#include "ray/util/logging.h"

namespace ray {
namespace scheduling {

SchedulerClient::SchedulerClient(const std::string &address) {
  std::vector<std::string> addr = absl::StrSplit(address, ':');
  RAY_CHECK(addr.size() == 2);
  address_ = addr[0];
  port_ = std::stoi(addr[1]);
}

Status SchedulerClient::Connect(instrumented_io_context &io_service) {
  RAY_CHECK(!is_connected_);
  client_call_manager_ = std::make_unique<rpc::ClientCallManager>(io_service);
  grpc_client_ = std::make_shared<rpc::scheduling::SchedulerRpcClient>(
      address_, port_, *client_call_manager_);

  is_connected_ = true;

  RAY_LOG(INFO) << "Connected to scheduler at " << address_ << ":" << port_
                << ".";
  return Status::OK();
}

void SchedulerClient::Disconnect() {
  if (!is_connected_) {
    RAY_LOG(WARNING) << "SchedulerClient is already disconnected.";
    return;
  }
  is_connected_ = false;
  RAY_LOG(INFO) << "SchedulerClient disconnected.";
}

Status SchedulerClient::ScheduleAndDispatchTasks() { return Status::OK(); }

Status SchedulerClient::QueueAndScheduleTask() { return Status::OK(); }

Status SchedulerClient::CancelTask() { return Status::OK(); }

}  // namespace scheduling
}  // namespace ray
