require "../src/neo4j"
require "../src/neo4j/cluster"
require "uuid"

host = "neo4j-helm-neo4j-core-2.neo4j-helm-neo4j.default.svc.cluster.local"
uri = URI.parse("bolt+routing://neo4j:password@#{host}")

struct Foo
  Neo4j.map_node id: UUID
end

cluster = Neo4j::Cluster.new(entrypoint: uri, ssl: false)

puts "*****WRITING*****"
cluster.write_transaction do |txn|
  # What kind of Neo4j server are we on?
  puts txn.exec_cast_scalar("call dbms.cluster.role", String)

  # Create a node and print it out
  pp txn.exec_cast <<-CYPHER, {Foo}
    CREATE (foo:Foo { id: randomUUID() })
    RETURN foo
  CYPHER
end

5.times { puts }
puts "*****READING*****"
cluster.read_transaction do |txn|
  # What kind of Neo4j server are we on?
  puts txn.exec_cast_scalar("call dbms.cluster.role", String)

  txn.exec_cast "match (n) return n", {Foo} do |(foo)|
    # Print each result as it comes back from the DB
    pp foo
  end
end
