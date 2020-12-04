require "../spec_helper"

require "../../src/neo4j"

if ENV["NEO4J_URL"]?
  uri = URI.parse(ENV["NEO4J_URL"])

  run_specs Neo4j.connect(uri, ssl: !!ENV["NEO4J_SSL"]?)
else
  puts "Set NEO4J_URL (and NEO4J_SSL if you need SSL) in order to run driver specs"
end

def run_specs(neo4j : Neo4j::DirectDriver)
  describe Neo4j::DirectDriver do
    # TODO: Add specs for DirectDriver
  end
end

def run_specs(neo4j : Neo4j::Cluster)
  describe Neo4j::Cluster do
    it "does cluster things" do
      neo4j.read_query <<-CYPHER, as: {UUID} do |result|
        UNWIND range(1, 100) AS index
        RETURN randomUUID()
      CYPHER
        # Do something with UUIDs
      end

      neo4j.session(&.read_transaction(&.exec_cast(<<-CYPHER, {UUID}) { |(uuid)|
        UNWIND range(1, 10) AS index
        RETURN randomUUID()
      CYPHER
        pp uuid
      }))
    end
  end
end
