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

        it "handles exceptions" do
          pool.connection do |connection|
            begin
              connection.execute "omg lol"
            rescue Neo4j::QueryException
              # we did this on purpose
            end

            result = connection.execute "return 42"

            result.first.first.should eq 42
          end
        end

        describe "transactions" do
          it "yields a transaction" do
            pool.connection do |connection|
              begin
                connection.transaction do |t|
                  t.should be_a Transaction

                  # Provides the same execute API as the connection
                  result = t.execute "RETURN $value", value: 42
                  result.first.first.should eq 42

                  result = t.execute "RETURN $value", { "value" => 42 }
                  result.first.first.should eq 42
                end

                # Without the block param
                connection.transaction do
                  result = connection.execute "RETURN 42"
                  result.first.first.should eq 42
                end
              end
            end
          end

          it "rolls back the transaction if an error occurs" do
            pool.connection do |connection|
              id = nil

              begin
                connection.transaction do |t|
                  # Initial query whose result should not exist outside this
                  # block after our exception below.
                  id = t
                    .execute("CREATE (u:User) RETURN ID(u)")
                    .first
                    .first

                  t.execute "break everything please"
                end
              rescue ex : QueryException
              end

              id.should_not be_nil

              result = connection.execute "MATCH (u) WHERE ID(u) = $id RETURN u", id: id
              result.any?.should be_false
            end # connection
          end # it rolls back

          it "allows you to roll back a transaction explicitly" do
            pool.connection do |connection|
              connection.transaction do |t|
                id = t
                  .execute("CREATE (u:User) RETURN ID(u)")
                  .first
                  .first

                t.rollback

                raise "This should never run"
              end
            end
          end

          it "does not allow nested transactions" do
            pool.connection do |connection|
              expect_raises NestedTransactionError do
                connection.transaction do |t|
                  connection.transaction do |t2|
                    raise "We should never be able to enter this block"
                  end
                end
              end
            end
          end
        end

        it "deserializes values" do
          pool.connection do |connection|
            connection.execute(<<-CYPHER).first.tap do |(datetime, point2d, latlng, point3d)|
              RETURN
                datetime(),
                point({ x: 1, y: 2 }),
                point({ latitude: 39, longitude: -76 }),
                point({ x: 1, y: 2, z: 3 })
            CYPHER
              datetime.should be_a Time
              point2d.should eq Point2D.new(x: 1, y: 2)
              point3d.should eq Point3D.new(x: 1, y: 2, z: 3)
              latlng.should eq LatLng.new(latitude: 39, longitude: -76)
            end
          end
        end
      end
    end
  end
end
