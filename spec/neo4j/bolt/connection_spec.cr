require "../../../spec_helper"
require "uuid"
require "pool/connection"

require "../../../../src/neo4j/bolt/connection"

module Neo4j
  module Bolt
    run_integration_specs = ENV["NEO4J_URL"]?

    if run_integration_specs.nil?
      puts "Skipping integration specs. Set NEO4J_URL environment variable to an active Neo4j database to run them."
    else
      pool = ConnectionPool.new(capacity: 25) do
        Connection.new(ENV["NEO4J_URL"], ssl: !!ENV["NEO4J_USE_SSL"]?)
      end

      describe Connection do
        it "talks to a real DB" do
          uuid = UUID.random.to_s
          pool.connection do |connection|
            connection.execute <<-CYPHER, uuid: uuid, name: "Hello world"
              CREATE (user:User {
                id: $uuid,
                name: $name
              })
            CYPHER

            result = connection.execute(<<-CYPHER, uuid: uuid)
              MATCH (user:User { id: $uuid })
              RETURN user
              LIMIT 1
            CYPHER

            node = result.first.first.as(Neo4j::Node)
            node.properties["name"].should eq "Hello world"

            connection.execute <<-CYPHER, uuid: uuid
              MATCH (user:User { id: $uuid })
              DELETE user
            CYPHER
          end
        end

        it "handles nodes and relationships" do
          user_id = UUID.random.to_s
          group_id = UUID.random.to_s
          now = Time.now.to_unix

          pool.connection do |connection|
            result = connection.execute <<-CYPHER, user_id: user_id, group_id: group_id, now: now
              CREATE (user:User { id: $user_id, name: "Foo Bar" })
              CREATE (group:Group { id: $group_id, name: "Test Group" })
              CREATE (user)-[membership:MEMBER_OF { joined_at: $now }]->(group)
              RETURN user, membership, group
            CYPHER

            user, membership, group = result.data.first

            membership.as(Neo4j::Relationship).properties["joined_at"].as(Int32).should eq now

            connection.execute <<-CYPHER, user_id: user_id, group_id: group_id
              MATCH (user:User { id: $user_id })-[membership:MEMBER_OF]->(group:Group { id: $group_id })
              DELETE user, membership, group
            CYPHER
          end
        end
      end
    end
  end
end
