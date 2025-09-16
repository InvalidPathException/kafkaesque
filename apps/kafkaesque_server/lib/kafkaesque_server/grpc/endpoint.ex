defmodule KafkaesqueServer.GRPC.Endpoint do
  use GRPC.Endpoint

  intercept GRPC.Server.Interceptors.Logger
  run(Kafkaesque.GRPC.Service)
end
