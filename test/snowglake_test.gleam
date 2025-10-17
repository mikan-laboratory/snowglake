import gleam/int
import gleam/list
import gleam/string
import gleeunit
import gleeunit/should
import snowglake

pub fn main() {
  gleeunit.main()
}

// gleeunit test functions end in `_test`
pub fn snowglake_generate_test() {
  let epoch = 1_420_070_400_000
  let worker_id = 20
  let process_id = 30

  let assert Ok(generator) =
    snowglake.new_generator()
    |> snowglake.with_epoch(epoch)
    |> snowglake.with_worker_id(worker_id)
    |> snowglake.with_process_id(process_id)
    |> snowglake.start()

  let id = generator |> snowglake.generate()

  id |> int.to_string() |> string.length() |> should.equal(19)
  let ts = id |> snowglake.timestamp(epoch)
  should.be_true(ts <= snowglake.get_now_milliseconds())
  id |> snowglake.worker_id |> should.equal(worker_id)
  id |> snowglake.process_id |> should.equal(process_id)

  generator |> snowglake.stop()
}

pub fn snowglake_generate_multiple_test() {
  let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()

  list.range(1, 5000)
  |> list.map(fn(_) { generator |> snowglake.generate() })
  |> list.unique()
  |> list.length()
  |> should.equal(5000)

  generator |> snowglake.stop()
}

pub fn snowglake_generate_future_epoch_test() {
  let epoch = snowglake.get_now_milliseconds() + 1000

  snowglake.new_generator()
  |> snowglake.with_epoch(epoch)
  |> snowglake.start()
  |> should.be_error()
}

pub fn snowgeam_generate_lazy_test() {
  let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()

  let id = generator |> snowglake.generate_lazy()

  id |> int.to_string() |> string.length() |> should.equal(19)
  let ts = id |> snowglake.timestamp(snowglake.default_epoch)
  should.be_true(ts <= snowglake.get_now_milliseconds())

  list.range(1, 5000)
  |> list.map(fn(_) { generator |> snowglake.generate_lazy() })
  |> list.unique()
  |> list.length()
  |> should.equal(5000)

  generator |> snowglake.stop()
}

pub fn snowglake_generate_lazy_with_set_timestamp_test() {
  let assert Ok(generator) =
    snowglake.new_generator()
    |> snowglake.with_timestamp(1_719_440_739_000)
    |> snowglake.start()

  let id = generator |> snowglake.generate_lazy()

  id |> int.to_string() |> string.length() |> should.equal(19)
  should.equal(id |> int.to_string(), "1806091479806902272")

  generator |> snowglake.stop()
}

pub fn snowglake_generate_many_test() {
  let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()

  let ids = generator |> snowglake.generate_many(5000)

  ids |> list.unique() |> list.length() |> should.equal(5000)
  let assert Ok(last_id) = ids |> list.last()

  last_id |> int.to_string() |> string.length() |> should.equal(19)
  should.be_true(
    last_id |> snowglake.timestamp(snowglake.default_epoch)
    <= snowglake.get_now_milliseconds(),
  )

  generator |> snowglake.stop()
}

pub fn snowglake_generate_many_lazy_test() {
  let time = 1_719_440_739_000

  let assert Ok(generator) =
    snowglake.new_generator()
    |> snowglake.with_timestamp(time)
    |> snowglake.with_worker_id(20)
    |> snowglake.with_process_id(30)
    |> snowglake.start()

  let ids = generator |> snowglake.generate_many_lazy(5000)

  ids |> list.unique() |> list.length() |> should.equal(5000)
  let assert Ok(last_id) = ids |> list.last()

  last_id |> int.to_string() |> string.length() |> should.equal(19)
  should.equal(
    last_id |> snowglake.timestamp(snowglake.default_epoch),
    time + 1,
  )
  should.equal(last_id |> int.to_string(), "1806091479813841799")

  generator |> snowglake.stop()
}
