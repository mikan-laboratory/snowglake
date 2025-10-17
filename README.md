# Snowglake ❄️

[![Package Version](https://img.shields.io/hexpm/v/snowglake)](https://hex.pm/packages/snowglake)
[![Hex Docs](https://img.shields.io/badge/hex-docs-ffaff3)](https://hexdocs.pm/snowglake/)

Gleam version of the Twitter's [snowflake](https://github.com/twitter-archive/snowflake/tree/snowflake-2010) format for unique IDs.
Although it is more inspired by the Discord [implementation](https://discord.com/developers/docs/reference#snowflakes) of the snowflake format.

They are great for generating unique IDs in a distributed system.

This package is based on the [`snowgleam`](https://hexdocs.pm/snowgleam/index.html) library, updated to work with the latest Gleam releases.

```sh
gleam add snowglake
```
And then use it in your project like this:
```gleam
import gleam/io
import gleam/int
import snowglake

pub fn main() {
  let assert Ok(generator) = snowglake.new_generator() |> snowglake.start()
  let id = generator |> snowglake.generate()
  io.println("Generated ID: " <> id |> int.to_string())

  generator |> snowglake.stop()
}
```

## Features
- Setup with a custom epoch
- Simple ID generation
- Lazy ID generation
- Lazy ID generation with custom timestamp (useful for backfilling)
- Batch ID generation
- Batch Lazy ID generation

Further documentation can be found at <https://hexdocs.pm/snowglake>.
