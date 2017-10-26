require "uuid"

require "./src/neo4j"

connection = Neo4j::Bolt::Connection.new("bolt://neo4j:password@localhost", ssl: false)

class User
  Neo4j.map_node(
    uuid: UUID,
    handle: String,
    name: String,
    created_at: Time,
  )
end

class Post
  Neo4j.map_node(
    uuid: UUID,
    body: String,
    public: Bool,
    created_at: Time,
    updated_at: Time,
  )
end

query = <<-CYPHER
  MATCH (self:User)-[r:FOLLOWS]->(author:User)-[:IN_OUTBOX]->(activity)-[:WRAPS]->(object)
  WHERE self.uuid = $uuid
  RETURN author, collect(object), r
CYPHER

start = Time.now
result = connection.execute(
  "MATCH (user:User { uuid: $uuid }) RETURN user",
  { "uuid" => "d9c0cfd6-8678-44f0-9112-87b019406e87" }
)
finish = Time.now

result.each do |(user)|
  pp user
end
puts "Query loaded and processed in #{finish - start}"
