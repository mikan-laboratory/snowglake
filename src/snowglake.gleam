//// A module for generating unique IDs using the Twitter Snowflake algorithm.

import gleam/erlang/process
import gleam/int
import gleam/list
import gleam/otp/actor
import gleam/result
import gleam/string
import gleam/time/timestamp

/// The default epoch for the generator. Corresponds to the Twitter epoch.
pub const default_epoch: Int = 1_288_834_974_657

/// The maximum number of IDs that can be generated in a single millisecond.
const max_index: Int = 4096

/// Type of the generator. It is the actual
/// public interface for the generator and should be used to interact with it.
/// It holds the actor subject that is used to handle the generator state.
///
/// # Examples
/// ```gleam
/// import gleam/snowglake
///
/// pub type Context {
///   Context(generator: snowglake.Generator)
/// }
///
/// let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()
/// let context = Context(generator: generator)
/// let id = context.generator |> snowglake.generate()
/// ```
pub opaque type Generator {
  Generator(subject: process.Subject(Message))
}

/// The messages that the generator can receive.
pub opaque type Message {
  Generate(reply_with: process.Subject(Int))
  GenerateLazy(reply_with: process.Subject(Int))
  GenerateMany(Int, process.Subject(List(Int)))
  GenerateManyLazy(Int, process.Subject(List(Int)))
  Shutdown
}

/// The Snowflake ID generator node.
/// A node holds the state of the generator and is used to generate IDs.
pub opaque type Node {
  Node(epoch: Int, worker_id: Int, process_id: Int, last_ts: Int, index: Int)
}

/// Creates a new Snowflake ID generator with default settings.
pub fn new_generator() -> Node {
  Node(epoch: default_epoch, worker_id: 0, process_id: 0, last_ts: 0, index: -1)
}

/// Sets the epoch for the generator.
pub fn with_epoch(node: Node, epoch: Int) -> Node {
  Node(..node, epoch: epoch)
}

/// Sets the worker ID for the generator.
pub fn with_worker_id(node: Node, worker_id: Int) -> Node {
  Node(..node, worker_id: worker_id)
}

/// Sets the process ID for the generator.
pub fn with_process_id(node: Node, process_id: Int) -> Node {
  Node(..node, process_id: process_id)
}

/// Sets timestamp for the generator. Useful for lazy generation. It should not
/// be used along with normal generation.
pub fn with_timestamp(node: Node, last_ts: Int) -> Node {
  Node(..node, last_ts: last_ts)
}

/// Starts the generator.
pub fn start(node: Node) -> Result(Generator, String) {
  case node.epoch > get_now_milliseconds() {
    True -> Error("epoch must be in the past")
    False -> {
      let node = case node.last_ts {
        0 -> Node(..node, last_ts: node |> get_timestamp)
        _ -> Node(..node, last_ts: node.last_ts |> int.subtract(node.epoch))
      }

      node
      |> actor.new
      |> actor.on_message(handle_message)
      |> actor.start
      |> result.map_error(format_start_error)
      |> result.map(fn(started) { Generator(subject: started.data) })
    }
  }
}

fn format_start_error(error: actor.StartError) -> String {
  let detail = case error {
    actor.InitTimeout -> "initialisation timed out"
    actor.InitFailed(reason) -> "initialisation failed: " <> reason
    actor.InitExited(reason) ->
      "initialisation exited: " <> string.inspect(reason)
  }

  "could not start actor: " <> detail
}

/// Generates a new Snowflake ID.
///
/// # Examples
/// ```gleam
/// import gleam/snowglake
///
/// let epoch = 1_420_070_400_000
/// let worker_id = 12
/// let process_id = 1
///
/// let assert Ok(generator) =
///   snowglake.new_generator()
///   |> snowglake.with_epoch(epoch)
///   |> snowglake.with_worker_id(worker_id)
///   |> snowglake.with_process_id(process_id)
///   |> snowglake.start()
///
/// let id = snowglake.generate(generator)
/// ```
pub fn generate(generator: Generator) -> Int {
  actor.call(generator.subject, 10, Generate)
}

/// Generates a new Snowflake ID lazily.
/// It works like the `generate` function but it does not uses the current
/// timestamp, instead it consumes all the 4096 IDs of every millisecond.
/// It may be faster and useful in some cases than the `generate` function.
/// For example, to generate a batch of IDs or to generate IDs for a particular
/// time.
pub fn generate_lazy(generator: Generator) -> Int {
  actor.call(generator.subject, 10, GenerateLazy)
}

/// Generates many Snowflake IDs.
///
/// # Examples
/// ```gleam
/// import gleam/snowglake
///
/// let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()
/// let ids = snowglake.generate_many(generator, 5000)
/// ```
pub fn generate_many(generator: Generator, count: Int) -> List(Int) {
  actor.call(generator.subject, 100, fn(reply) { GenerateMany(count, reply) })
}

/// Generates many Snowflake IDs lazily.
///
/// # Examples
/// ```gleam
/// import gleam/snowglake
///
/// let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()
/// let ids = snowglake.generate_many_lazy(generator, 5000)
/// ```
pub fn generate_many_lazy(generator: Generator, count: Int) -> List(Int) {
  actor.call(generator.subject, 100, fn(reply) {
    GenerateManyLazy(count, reply)
  })
}

/// Stops the generator.
pub fn stop(generator: Generator) {
  actor.send(generator.subject, Shutdown)
}

/// Actor message handler.
fn handle_message(node: Node, message: Message) -> actor.Next(Node, Message) {
  case message {
    Generate(reply) -> {
      let node = node |> setup
      let id = node |> generate_id
      actor.send(reply, id)
      actor.continue(node)
    }
    GenerateLazy(reply) -> {
      let node = node |> lazy_setup
      let id = node |> generate_id
      actor.send(reply, id)
      actor.continue(node)
    }
    GenerateMany(count, reply) -> {
      let node = node |> setup
      let #(node, ids) = node |> generate_ids(count)
      actor.send(reply, ids |> list.reverse())
      actor.continue(node)
    }
    GenerateManyLazy(count, reply) -> {
      let #(node, ids) = node |> generate_ids(count)
      actor.send(reply, ids |> list.reverse())
      actor.continue(node)
    }
    Shutdown -> actor.stop()
  }
}

/// Generates a new Snowflake ID.
fn generate_id(node: Node) -> Int {
  int.bitwise_shift_left(node.last_ts, 22)
  |> int.bitwise_or(int.bitwise_shift_left(node.worker_id, 17))
  |> int.bitwise_or(int.bitwise_shift_left(node.process_id, 12))
  |> int.bitwise_or(node.index)
}

/// Generates many Snowflake IDs.
fn generate_ids(node: Node, count: Int) -> #(Node, List(Int)) {
  case count {
    0 -> #(node, [])
    _ -> {
      let node = node |> lazy_setup
      let id = node |> generate_id
      let #(node, ids) = node |> generate_ids(count - 1)
      #(node, list.append(ids, [id]))
    }
  }
}

/// Sets up the node before generating a new ID.
/// Handles the case where multiple IDs are generated in the same millisecond.
/// It wait for the next millisecond if the 4096 were already generated.
fn setup(node: Node) -> Node {
  let timestamp = node |> get_timestamp
  case node {
    Node(last_ts: lts, index: i, ..) if lts == timestamp && i < max_index -> {
      Node(..node, index: i + 1)
    }
    Node(last_ts: lts, ..) if lts == timestamp -> node |> setup
    _ -> Node(..node, index: 0, last_ts: timestamp)
  }
}

/// Lazily sets up the node before generating a new ID.
/// It does not uses current timestamp to generate the next ID. It consumes
/// all the 4096 of every millisecond before moving to the next one.
/// It may be faster and useful in some cases.
fn lazy_setup(node: Node) -> Node {
  let i = { node.index + 1 } % max_index
  case i {
    i if i == 0 && node.index != -1 ->
      Node(..node, index: i, last_ts: node.last_ts + 1)
    _ -> Node(..node, index: i)
  }
}

/// Extracts the timestamp from a Snowflake ID using the provided epoch.
pub fn timestamp(id: Int, epoch: Int) -> Int {
  id |> int.bitwise_shift_right(22) |> int.add(epoch)
}

/// Extracts the worker ID from a Snowflake ID.
pub fn worker_id(id: Int) -> Int {
  id |> int.bitwise_and(0x3E0000) |> int.bitwise_shift_right(17)
}

/// Extracts the process ID from a Snowflake ID.
pub fn process_id(id: Int) -> Int {
  id |> int.bitwise_and(0x1F000) |> int.bitwise_shift_right(12)
}

/// Gets the current timestamp using erlang os:system_time/1.
fn get_timestamp(node: Node) -> Int {
  get_now_milliseconds() |> int.subtract(node.epoch)
}

pub fn get_now_milliseconds() -> Int {
  let now = timestamp.system_time()
  let #(seconds, nanoseconds) = timestamp.to_unix_seconds_and_nanoseconds(now)

  let milliseconds_from_nanoseconds = case int.divide(nanoseconds, 1_000_000) {
    Ok(value) -> value
    Error(_) ->
      panic as "unexpected divide-by-zero while computing milliseconds"
  }

  let ts_ms_int = seconds * 1000 + milliseconds_from_nanoseconds

  ts_ms_int
}
