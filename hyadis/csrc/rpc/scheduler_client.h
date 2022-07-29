#pragma once

#include <grpcpp/grpcpp.h>

#include <thread>

#include "hyadis/csrc/protobuf/scheduler.grpc.pb.h"
#include "hyadis/csrc/protobuf/scheduler.pb.h"
#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"

namespace ray {

namespace rpc {

namespace scheduling {

class SchedulerRpcClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the scheduler server.
  /// \param[in] port Port of the scheduler server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing
  /// requests.
  SchedulerRpcClient(const std::string &address, const int port,
                     ClientCallManager &client_call_manager) {
    grpc_client_ = std::make_unique<GrpcClient<SchedulerService>>(
        address, port, client_call_manager);
  };

  VOID_RPC_CLIENT_METHOD(SchedulerService, Hello, grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(SchedulerService, QueueAndScheduleTask, grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(SchedulerService, ScheduleAndDispatchTasks,
                         grpc_client_,
                         /*method_timeout_ms*/ -1, )

  VOID_RPC_CLIENT_METHOD(SchedulerService, CancelTask, grpc_client_,
                         /*method_timeout_ms*/ -1, )

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<SchedulerService>> grpc_client_;
};

}  // namespace scheduling
}  // namespace rpc
}  // namespace ray
