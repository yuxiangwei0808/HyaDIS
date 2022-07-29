#pragma once

#include "hyadis/csrc/protobuf/scheduler.grpc.pb.h"
#include "hyadis/csrc/protobuf/scheduler.pb.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

namespace ray {

namespace rpc {

namespace scheduling {

#define HYADIS_SCHEDULER_RPC_HANDLERS                                 \
  RPC_SERVICE_HANDLER(SchedulerService, Hello, -1)                    \
  RPC_SERVICE_HANDLER(SchedulerService, ScheduleAndDispatchTasks, -1) \
  RPC_SERVICE_HANDLER(SchedulerService, QueueAndScheduleTask, -1)     \
  RPC_SERVICE_HANDLER(SchedulerService, CancelTask, -1)

class SchedulerServiceHandler {
 public:
  virtual ~SchedulerServiceHandler() {}
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request
  /// is done.
  // HYADIS_SCHEDULER_DECLARE_RPC_HANDLERS
  virtual void HandleHello(const HelloRequest &request, HelloReply *reply,
                           SendReplyCallback send_reply_callback) = 0;
  virtual void HandleScheduleAndDispatchTasks(
      const ScheduleAndDispatchTasksRequest &request,
      ScheduleAndDispatchTasksReply *reply,
      SendReplyCallback send_reply_callback) = 0;
  virtual void HandleQueueAndScheduleTask(
      const QueueAndScheduleTaskRequest &request,
      QueueAndScheduleTaskReply *reply,
      SendReplyCallback send_reply_callback) = 0;
  virtual void HandleCancelTask(const CancelTaskRequest &request,
                                CancelTaskReply *reply,
                                SendReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `SchedulerService`.
class SchedulerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  SchedulerGrpcService(instrumented_io_context &io_service,
                       SchedulerServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories)
      override {
    HYADIS_SCHEDULER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  SchedulerService::AsyncService service_;

  /// The service handler that actually handle the requests.
  SchedulerServiceHandler &service_handler_;
};

}  // namespace scheduling
}  // namespace rpc
}  // namespace ray
