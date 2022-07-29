#pragma once

#include "hyadis/csrc/protobuf/scheduler.pb.h"
#include "hyadis/csrc/rpc/scheduler_client.h"
#include "hyadis/csrc/rpc/scheduler_server.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/status.h"
#include "ray/common/task/task.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/rpc/server_call.h"
#include "ray/util/util.h"

namespace ray {

namespace scheduling {

struct SchedulerConfig {
  std::string grpc_server_name = "Scheduler";
  uint16_t grpc_server_port = 0;
  uint16_t grpc_server_thread_num = 1;
  std::string node_ip_address;
  std::string log_dir;
};

class Scheduler : public rpc::scheduling::SchedulerServiceHandler {
 public:
  Scheduler(const SchedulerConfig &config,
            instrumented_io_context &main_service,
            std::shared_ptr<gcs::GcsClient> gcs_client);

  void Start();

  void Stop();

  int GetPort() const { return rpc_server_.GetPort(); }

  void HandleHello(const rpc::scheduling::HelloRequest &request,
                   rpc::scheduling::HelloReply *reply,
                   rpc::SendReplyCallback send_reply_callback) {
    std::string msg = "233";
    RAY_LOG(INFO) << "This is hyadis scheduler:" << msg;
    reply->set_message(msg);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  void HandleQueueAndScheduleTask(
      const rpc::scheduling::QueueAndScheduleTaskRequest &request,
      rpc::scheduling::QueueAndScheduleTaskReply *reply,
      rpc::SendReplyCallback send_reply_callback);

  void HandleScheduleAndDispatchTasks(
      const rpc::scheduling::ScheduleAndDispatchTasksRequest &request,
      rpc::scheduling::ScheduleAndDispatchTasksReply *reply,
      rpc::SendReplyCallback send_reply_callback);

  void HandleCancelTask(const rpc::scheduling::CancelTaskRequest &request,
                        rpc::scheduling::CancelTaskReply *reply,
                        rpc::SendReplyCallback send_reply_callback);

 private:
  /// Queue task and schedule. This hanppens when processing the worker lease
  /// request.
  ///
  /// \param task: The incoming task to be queued and scheduled.
  /// \param grant_or_reject: True if we we should either grant or reject the
  /// request
  ///                         but no spillback.
  /// \param is_selected_based_on_locality : should schedule on local node if
  /// possible. \param reply: The reply of the lease request. \param
  /// send_reply_callback: The function used during dispatching.
  // void QueueAndScheduleTask(const RayTask &task, bool grant_or_reject,
  //                           bool is_selected_based_on_locality,
  //                           rpc::RequestWorkerLeaseReply *reply,
  //                           rpc::SendReplyCallback send_reply_callback);

  // /// Attempt to cancel an already queued task.
  // ///
  // /// \param task_id: The id of the task to remove.
  // /// \param failure_type: The failure type.
  // ///
  // /// \return True if task was successfully removed. This function will
  // return
  // /// false if the task is already running.
  // bool CancelTask(
  //     const TaskID &task_id,
  //     rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type =
  //         rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED,
  //     const std::string &scheduling_failure_message = "");

  // Schedule and dispatch tasks.
  void ScheduleAndDispatchTasks();

  const SchedulerConfig config_;

  instrumented_io_context &main_service_;

  rpc::GrpcServer rpc_server_;

  rpc::scheduling::SchedulerGrpcService rpc_service_;

  rpc::ClientCallManager client_call_manager_;

  std::shared_ptr<gcs::GcsClient> gcs_client_;

  PeriodicalRunner periodical_runner_;
};

class SchedulerClient : public std::enable_shared_from_this<SchedulerClient> {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the scheduler server.
  SchedulerClient(const std::string &address);

  ~SchedulerClient() = default;

  Status Connect(instrumented_io_context &io_service);

  void Disconnect();

  void Hello() {
    rpc::scheduling::HelloRequest request;
    grpc_client_->Hello(request, [](const Status &status,
                                    const rpc::scheduling::HelloReply &reply) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Error saying hello:" << status;
      } else {
        std::cout << "Received reply: " << reply.message() << std::endl;
      };
    });
  }

  Status ScheduleAndDispatchTasks();

  Status QueueAndScheduleTask();

  Status CancelTask();

 private:
  /// The RPC client.
  std::shared_ptr<rpc::scheduling::SchedulerRpcClient> grpc_client_;
  std::unique_ptr<rpc::ClientCallManager> client_call_manager_;

  // Scheduler address
  std::string address_;
  int port_ = 0;

 protected:
  std::atomic<bool> is_connected_{false};
};

}  // namespace scheduling
}  // namespace ray
