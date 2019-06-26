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

# The `ssl` option defaults to `true` so you don't accidentally send the
# password to your production DB in cleartext.
connection = Neo4j::Bolt::Connection.new(
  "bolt://neo4j:password@localhost:7687",
  ssl: false,
)
```

The `connection` has the following public methods:

- `execute(query : String, params = Neo4j::Map.new) : Neo4j::Result`
- `stream(query : String, params = Neo4j::Map.new) : Neo4j::StreamingResult`
- `exec_cast(query : String, params : Neo4j::Map, types : Tuple(*TYPES) : Neo4j::Result`
- `transaction(&block)`
- `reset`

### `execute(query : String, params = ({} of String => Neo4j::Type))`

Executes the given Cypher query. Takes a hash of params for sanitization and query caching.

```crystal
result = connection.execute("
  MATCH (order:Order)-[:ORDERS]->(product:Product)
  RETURN order, collect(product)
  LIMIT 10
")
```

This method returns a `Neo4j::Result`. You can iterate over it with `Enumerable` methods. Each iteration of the block will return an array of the values passed to the query's `RETURN` clause:

```crystal
result = connection.execute(<<-CYPHER, { "email" => "foo@example.com" })
  MATCH (self:User)<-[:SENT_TO]-(message:Message)-[:SENT_BY]->(author:User)
  WHERE self.email == $email
  RETURN author, message
CYPHER

result.map do |(author, message)| # Using () to destructure the array into block args
  do_something(
    author: author.as(Neo4j::Node),
    message: message.as(Neo4j::Node),
  )
end
```

Note that we cast the values returned from the query into `Neo4j::Node`. Each value returned from a query can be any Neo4j data type and cannot be known at compile time, so we have to cast the values into the types we know them to be — in this case, we are returning nodes.

### `transaction(&block)`

Executes the block within the context of a Neo4j transaction. At the end of the block, the transaction is committed. If an exception is raised, the transaction will be rolled back and the connection will be reset to a clean state.

Example:

```crystal
connection.transaction do
  query = <<-CYPHER
    CREATE (user:User {
      uuid: $uuid,
      name: $name,
      email: $email,
    })
  CYPHER

  connection.execute(query, params.merge({ "uuid" => UUID.random.to_s }))
end
```

### `stream(query : String, parameters : Hash(String, Neo4j::Type))` _EXPERIMENTAL_

Behaves similar to `execute(query, parameters)`, but the results are streamed rather than evaluated eagerly. For large result sets, this can drastically reduce memory usage and eliminates the need to provide workarounds like [ActiveRecord's `find_each`](https://api.rubyonrails.org/classes/ActiveRecord/Batches.html#method-i-find_each) method.

Example:

```crystal
struct User
  Neo4j.map_node(
    id: UUID,
    email: String,
    name: String,
    created_at: Time,
  )
end

connection
  .stream("MATCH (user:User) RETURN user")
  .each
  .map { |(user_node)| User.new(user_node.as(Neo4j::Node)) }
```

In this example, the driver will not retrieve a result from the connection until it is needed. In many cases, this reduces memory consumption as the values returned from the database are not all stored in memory at once. Consider the eager version:

```crystal
connection
  .execute("MATCH (user:User) RETURN user")
  .map { |(user_node)| User.new(user_node.as(Neo4j::Node)) }
```

This code would need to keep all of the user nodes in your entire graph in memory at once while it builds the array of `User` objects created from those nodes.

Streaming results not only reduces memory usage, but also improves time to first result. Loading everything all at once means you can't process the first result until you have the last result. Streaming lets you process the first result before you've received the second.

**IMPORTANT:** The result stays inside the communication buffer until the application consumes it. If you are using a connection pool, it is important not to release the connection back to the pool until you've consumed the entire result set:

```crystal
CONNECTION_POOL = ConnectionPool(Neo4j::Bolt::Connection).new do
  Neo4j::Bolt::Connection.new(NEO4J_URL)
end

def fetch_posts(for topic : Topic) : Array(Post)
  CONNECTION_POOL.connection do |conn|
    results = conn.stream <<-CYPHER, topic_id: topic.id
      MATCH (topic : Topic { id: $topic_id })
      MATCH (post : Post)
      MATCH (post)-[:POSTED_TO]->(topic)

      RETURN post
    CYPHER

    # This lazily consumes all of the results, so when we exit this block, we
    # will not have consumed them. We need to eliminate the `each` here.
    results.each.map do |(post)|
      Post.new(post.as Neo4j::Node)
    end
  end
end

posts = fetch_posts for: topic
```

### `reset`

Resets a connection to a clean state. A connection will automatically call `reset` if an exception is raised within a transaction, so you shouldn't have to call this explicitly, but it's provided just in case.

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

### `Neo4j::Type`

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
Array(Neo4j::Type) |
Hash(String, Neo4j::Type) |
Neo4j::Node |
Neo4j::Relationship |
Neo4j::UnboundRelationship |
Neo4j::Path |
Neo4j::Success |
Neo4j::Failure |
Neo4j::Ignored
```

### Mapping to Domain Objects

Similar to `JSON.mapping` in the Crystal standard library, you can map nodes and relationships to domain objects. For example:

```crystal
require "uuid"

class User
  Neo4j.map_node(
    uuid: UUID,
    email: String,
    name: String
    registered_at: Time,
  )
end

class Product
  Neo4j.map_node(
    uuid: UUID,
    name: String,
    description: String,
    price: Int32,
    created_at: Time,
  )
end

class CartItem
  Neo4j.map_relationship(
    quantity: Int32,
    price: Int32,
  )
end
```

With these in place, you can build them from your nodes and relationships:

```crystal
result = connection.execute(<<-CYPHER, { "uuid" => params["uuid"] })
  MATCH (product:Product)-[cart_item:IN_CART]->(user:User { uuid: $uuid })
  RETURN product, cart_item
CYPHER

cart = Cart.new(result.map { |(product, cart_item)|
  {
    product: Product.new(product.as(Neo4j::Node)),
    cart_item: CartItem.new(cart_item.as(Neo4j::Relationship)),
  }
})
```

### Caveats/Limitations

- Certain types like `Point`, `Time`, `DateTime`, `LocalTime`, `LocalDateTime` are not available in Bolt yet
  - They will be added when the Bolt spec is updated to include them
- This is still relatively early development, so while it supports a lot of functionality, some APIs might change as improvements are discovered
  - For example, `transaction` doesn't yield a transaction and everything is just called on the connection, which is not ideal, so this is pretty likely to change

## Future development

- I'd like to support query pipelining
  - Send multiple queries, commands, etc, before reading the result from any of them. This could be useful for transactions where we don't try to consume the results until the end of the transaction.
  - The idea is that we don't spend time waiting on a response from a query if we can just pick it up later
  - If a query error occurs, the server will send `Ignored` results back to us until we acknowledge the failure, so there is no special processing needed for this occurrence
- Streaming results, letting us send a query and consume its result lazily
  - For example, if we query 1000 results, we could defer deserializing them until the application needs them, potentially saving on processing
  - I don't know if this would be the default
- `bolt+routing`
  - I'm checking out the Java driver to see how they handle routing between core servers in Enterprise clusters

## Acknowledgements/Credits

This implementation is _heavily_ based on [@benoist](https://github.com/benoist)'s [implementation of MessagePack](https://github.com/crystal-community/msgpack-crystal). I had never built a binary protocol parser in a statically typed language before, so it really helped jump-start the development of this project.

## Contributing

1. Fork it ( https://github.com/jgaskins/neo4j.cr/fork )
2. Create your feature branch (git checkout -b my-new-feature)
3. Commit your changes (git commit -am 'Add some feature')
4. Push to the branch (git push origin my-new-feature)
5. Create a new Pull Request

## Contributors

- [jgaskins](https://github.com/jgaskins) Jamie Gaskins - creator, maintainer
