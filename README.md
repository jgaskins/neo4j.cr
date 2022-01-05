# neo4j.cr

[![Join the chat at https://gitter.im/jgaskins/neo4j.cr](https://badges.gitter.im/jgaskins/neo4j.cr.svg)](https://gitter.im/jgaskins/neo4j.cr?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
<a href="https://travis-ci.org/jgaskins/neo4j.cr">
  <img src="https://travis-ci.org/jgaskins/neo4j.cr.svg?branch=master" alt="Check out the build on Travis CI">
</a>

Crystal implementation of a Neo4j driver using the Bolt protocol.

## Installation

Add this to your application's `shard.yml`:

```yaml
dependencies:
  neo4j:
    github: jgaskins/neo4j.cr
```

## Usage

First you need to set up a connection:

```crystal
require "neo4j"

neo4j_uri = URI.parse("bolt://neo4j:password@localhost:7687")

# The `ssl` option defaults to `true` so you don't accidentally send the
# password to your production DB in cleartext.
driver = Neo4j.connect(neo4j_uri, ssl: false)
```

This will return a cluster driver or a direct driver depending on whether you provided a `neo4j://` or `bolt://` URI, respectively. `neo4j://` can also be specified as `bolt+routing://`. Both drivers expose the same interface, but the cluster driver will route queries to a different server based on whether you specify that the query is a read or write query.

```crystal
struct Person
  include Neo4j::Serializable::Node

  getter id: UUID
  getter name: String
  getter email: String
end

driver.session do |session|
  session.read_transaction do |read|
    query = <<-CYPHER
      MATCH (person:Person { name: $name })
      RETURN person
    CYPHER

    read.exec_cast(query, {Person}, name: "Jamie") do |(person)|
      pp person
    end
  end

  session.write_transaction do |write|
    write.execute <<-CYPHER, name: "Jamie"
      MATCH (person:Person { name: $name })
      SET person.login_count = person.login_count + 1
    CYPHER
  end
end
```

### `Neo4j::Result`

- `type : (Neo4j::Success | Neo4j::Ignored)`
  - If you get an `Ignored` result, it probably means an error occurred. Call `connection#reset` to get it back to working order.
  - If a query results in a `Neo4j::Failure`, an exception is raised rather than wrapping it in a `Result`.
- `data : Array(Array(Neo4j::Type))`
  - This is the list of result values. For example, if you `RETURN a, b, c` from your query, then this will be an array of `[a, b, c]`.

The `Result` object itself is an `Enumerable`. Calling `Result#each` will iterate over the data for you.

### `Neo4j::Node`

These have a 1:1 mapping to nodes in your graph.

- `id : Int32`: the node's internal id
  - _WARNING_: Do not store this id anywhere. These ids can be reused by the database. If you need an application-level unique id, store a UUID on the node. It is useful in querying nodes connected to this one when you already have it in memory, but not beyond that.
- `labels : Array(String)`: the labels stored on your node
- `properties : Hash(String, Neo4j::Type)`: the properties assigned to this node

### `Neo4j::Relationship`

- `id: Int32`: the relationship's internal id
- `type : String`: the type of relationship
- `start : Int32`: the internal id for the node on the starting end of this relationship
- `end : Int32`: the internal id of the node this relationship points to
- `properties : Hash(String, Neo4j::Type)`: the properties assigned to this relationship

### `Neo4j::Value`

Represents any data type that can be stored in a Neo4j database and communicated via the Bolt protocol. It's a shorthand for this union type:

```crystal
Nil |
Bool |
String |
Int8 |
Int16 |
Int32 |
Int64 |
Float64 |
Time |
Neo4j::Point2D |
Neo4j::Point3D |
Neo4j::LatLng |
Array(Neo4j::Value) |
Hash(String, Neo4j::Value) |
Neo4j::Node |
Neo4j::Relationship |
Neo4j::UnboundRelationship |
Neo4j::Path
```

### Mapping to Domain Objects

Similar to `JSON.mapping` in the Crystal standard library, you can map nodes and relationships to domain objects. For example:

```crystal
require "uuid"

class User
  include Neo4j::Serializable::Node

  getter uuid: UUID
  getter email: String
  getter name: String
  getter registered_at: Time
end

class Product
  include Neo4j::Serializable::Node

  getter uuid: UUID
  getter name: String
  getter description: String
  getter price: Int32
  getter created_at: Time
end

class CartItem
  include Neo4j::Serializable::Relationship
  getter quantity: Int32
  getter price: Int32
end
```

## Acknowledgements/Credits

The implementation of the wire protocol is _heavily_ based on the [MessagePack shard](https://github.com/crystal-community/msgpack-crystal) to understand how to serialize and deserialize a binary protocol in Crystal.

## Contributing

1. Fork it ( https://github.com/jgaskins/neo4j.cr/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [jgaskins](https://github.com/jgaskins) Jamie Gaskins - creator, maintainer
