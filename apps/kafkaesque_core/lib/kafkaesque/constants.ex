defmodule Kafkaesque.Constants do
  @moduledoc """
  Shared constants used throughout the Kafkaesque system.
  """

  @magic_byte <<0x4B>>
  @frame_header_size 4
  @index_interval 10
  @index_bytes_threshold 4096
  @fsync_interval_ms 1000
  @default_batch_size 500
  @default_batch_timeout 5
  @max_key_size 256 * 1024
  @max_value_size 1024 * 1024
  @max_headers_size 32 * 1024
  @max_batch_size 1000

  def magic_byte, do: @magic_byte
  def frame_header_size, do: @frame_header_size
  def index_interval, do: @index_interval
  def index_bytes_threshold, do: @index_bytes_threshold
  def fsync_interval_ms, do: @fsync_interval_ms
  def default_batch_size, do: @default_batch_size
  def default_batch_timeout, do: @default_batch_timeout
  def max_key_size, do: @max_key_size
  def max_value_size, do: @max_value_size
  def max_headers_size, do: @max_headers_size
  def max_batch_size, do: @max_batch_size
end
