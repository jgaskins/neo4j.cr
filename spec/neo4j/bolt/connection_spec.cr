require "../../spec_helper"
require "uuid"

require "../../../src/neo4j/bolt/connection"
require "../../../src/neo4j/mapping"

struct TestNode
  Neo4j.map_node(
    id: UUID,
    name: String,
  )
end

struct Product
  Neo4j.map_node(
    id: UUID,
    name: String,
  )
end

struct Category
  Neo4j.map_node(
    id: UUID,
    name: String,
  )
end

struct SomethingElse
  Neo4j.map_node(
    id: UUID,
    name: String,
    foo: Int32,
  )
end

struct Zone
  Neo4j.map_node(
    polygon: Array(Neo4j::LatLng),
  )
end

struct Subscription
  Neo4j.map_node(
    id: UUID,
    amount_cents: Int32,
    last_billed_at: Time,
    next_bill_at: Time,
    frequency: Neo4j::Duration,
  )
end

struct User
  Neo4j.map_node(
    id: UUID,
    name: String,
  )
end

module Neo4j
  module Bolt
    run_integration_specs = ENV["NEO4J_URL"]?

    if run_integration_specs.nil?
      puts "Skipping integration specs. Set NEO4J_URL environment variable to an active Neo4j database to run them."
    else
      connection = Connection.new(ENV["NEO4J_URL"], ssl: !!ENV["NEO4J_USE_SSL"]?)

      describe Connection do
        it "talks to a real DB" do
          uuid = UUID.random.to_s
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

        it "allows passing a block to #execute" do
          values = Array(Int8).new
          result = connection.execute <<-CYPHER, start: 1, end: 10 do |(result)|
            UNWIND range($start, $end) AS index
            RETURN index
          CYPHER
            values << result.as Int8
          end

          values.should eq (1..10).to_a
        end

        it "handles nodes and relationships" do
          user_id = UUID.random.to_s
          group_id = UUID.random.to_s
          now = Time.now.to_unix

          result = connection.execute <<-CYPHER, user_id: user_id, group_id: group_id, now: now
            CREATE (user:User { id: $user_id, name: "Foo Bar" })
            CREATE (group:Group { id: $group_id, name: "Test Group" })
            CREATE (user)-[membership:MEMBER_OF { joined_at: $now }]->(group)
            RETURN user, membership, group
          CYPHER

          user, membership, group = result.first

          membership.as(Neo4j::Relationship).properties["joined_at"].as(Int32).should eq now

          connection.execute <<-CYPHER, user_id: user_id, group_id: group_id
            MATCH (user:User { id: $user_id })-[membership:MEMBER_OF]->(group:Group { id: $group_id })
            DELETE user, membership, group
          CYPHER
        end

        it "handles sending and receiving large queries" do
          big_array = (0..1_000_000).to_a
          result = connection.exec_cast "RETURN $value",
            { value: big_array },
            {Array(Int32)}

          result.first.first.should eq big_array
        end

        it "handles exceptions" do
          begin
            connection.execute "omg lol"
          rescue QueryException
            # we did this on purpose
          end

          begin
            connection.execute "create index on :Foo(id)"
            connection.execute "create constraint on (foo:Foo) assert foo.id is unique"
            raise Exception.new("Creating duplicate constraint did not ")
          rescue ex : IndexAlreadyExists
          rescue ex
            raise Exception.new("Expected IndexAlreadyExists, got #{ex.inspect}")
          ensure
            connection.execute "drop index on :Foo(id)"
          end

          result = connection.execute "return 42"

          result.first.first.should eq 42
        end

        describe "transactions" do
          it "yields a transaction" do
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

          it "rolls back the transaction if an error occurs" do
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
          end

          it "allows you to roll back a transaction explicitly" do
            connection.transaction do |t|
              id = t
                .execute("CREATE (u:User) RETURN ID(u)")
                .first
                .first

              t.rollback

              raise "This should never run"
            end
          end

          it "does not allow nested transactions" do
            expect_raises NestedTransactionError do
              connection.transaction do |t|
                connection.transaction do |t2|
                  raise "We should never be able to enter this block"
                end
              end
            end
          end
        end

        it "deserializes values" do
          connection.execute(<<-CYPHER).first.tap do |(datetime_offset, datetime_tz, duration, point2d, latlng, point3d)|
            RETURN
              datetime('2019-02-16T22:32:40.999-05:00'),
              datetime({
                year: 2019,
                month: 2,
                day: 16,
                hour: 22,
                minute: 32,
                second: 40,
                millisecond: 123,
                microsecond: 456,
                nanosecond: 789,
                timezone: 'America/New_York'
              }),
              duration({
                years: 1,
                months: 2,
                weeks: 3,
                days: 4,
                hours: 5,
                minutes: 6,
                seconds: 7,
                milliseconds: 8,
                microseconds: 9,
                nanoseconds: 10
              }),
              point({ x: 1, y: 2 }),
              point({ latitude: 39, longitude: -76 }),
              point({ x: 1, y: 2, z: 3 })
          CYPHER
            datetime_offset.should eq Time.new(
              year: 2019,
              month: 2,
              day: 16,
              hour: 22,
              minute: 32,
              second: 40,
              nanosecond: 999_000_000,
              location: Time::Location.load("America/New_York"),
            )
            datetime_tz.should eq Time.new(
              year: 2019,
              month: 2,
              day: 16,
              hour: 22,
              minute: 32,
              second: 40,
              nanosecond: 123_456_789,
              location: Time::Location.load("America/New_York"),
            )
            duration.should eq Duration.new(
              years: 1,
              months: 2,
              weeks: 3,
              days: 4,
              hours: 5,
              minutes: 6,
              seconds: 7,
              milliseconds: 8,
              microseconds: 9,
              nanoseconds: 10,
            )
            point2d.should eq Point2D.new(x: 1, y: 2)
            point3d.should eq Point3D.new(x: 1, y: 2, z: 3)
            latlng.should eq LatLng.new(latitude: 39, longitude: -76)
          end
        end

        it "streams results" do
          connection.transaction do |txn|
            connection.execute "MATCH (test:TestNode) DETACH DELETE test"

            3.times do |id|
              connection.execute "CREATE (:TestNode { id: $id })", id: id
            end

            results = connection.stream(<<-CYPHER)
              MATCH (test:TestNode) RETURN test
            CYPHER

            results.first # Consumes the first result
            results.count(&.itself).should eq 2 # So there are 2 left over
            results.count(&.itself).should eq 0 # Now there are none left

            txn.rollback
          end
        end

        describe "deserializing nodes as the specified type" do
          it "deserializes ints" do
            values = connection.exec_cast("return 12, 42, 500, 2000000000000", { Int8, Int16, Int32, Int64 })
            values.should be_a Array(Tuple(Int8, Int16, Int32, Int64))
            values.should eq [{ 12, 42, 500, 2_000_000_000_000 }]

            # Test multiple rows and multiple values returned
            values = connection.exec_cast(<<-CYPHER, { Int8, Int16 })
              UNWIND range(1, 2) AS value
              UNWIND range(1, 2) AS second_value
              RETURN value, second_value
            CYPHER
            values.should eq [{1, 1}, {1, 2}, {2, 1}, {2, 2}]
          end

          it "deserializes floats" do
            values = connection.exec_cast("return 6.9", { Float64 })
            values.should be_a Array(Tuple(Float64))
            values.should eq [{ 6.9 }]
          end

          it "deserializes strings and booleans" do
            values = connection.exec_cast "RETURN 'hello ' + $target, true",
              parameters: Map { "target" => "world" },
              types: { String, Bool }
            values.should eq [{ "hello world", true }]
          end

          it "deserializes spatial types" do
            values = connection.exec_cast(<<-CYPHER, { Point2D, LatLng, Point3D })
              RETURN
                point({ x: 69, y: 420 }),
                point({ latitude: 39, longitude: -76 }),
                point({ x: 1.1, y: 2.2, z: 3.3 })
            CYPHER
            p2d, latlng, p3d = values.first
            p2d.x.should eq 69
            p2d.y.should eq 420
            latlng.latitude.should eq 39.0
            latlng.longitude.should eq -76.0
            p3d.x.should eq 1.1
            p3d.y.should eq 2.2
            p3d.z.should eq 3.3
          end

          it "deserializes custom node types" do
            connection.transaction do |txn|
              id = connection
                .execute("CREATE (node:TestNode { id: randomUUID(), name: 'Test' }) RETURN node.id")
                .first
                .first
                .as(String)

              values = connection.exec_cast "MATCH (node:TestNode { id: $id }) RETURN node",
                { "id" => id },
                { TestNode }

              values.should be_a Array(Tuple(TestNode))
              values.first.first.name.should eq "Test"

              txn.rollback
            end
          end

          it "deserializes arrays" do
            connection.transaction do |txn|
              id = UUID.random.to_s

              connection.execute <<-CYPHER, id: id
                CREATE (category:Category {
                  id: $id,
                  name: "Stuff"
                })

                CREATE (product1:Product {
                  id: randomUUID(),
                  name: "Thing 1"
                })

                CREATE (product2:Product {
                  id: randomUUID(),
                  name: "Thing 2"
                })

                CREATE (product1)-[:IN_CATEGORY]->(category)
                CREATE (product2)-[:IN_CATEGORY]->(category)
              CYPHER

              results = connection.exec_cast(<<-CYPHER, { id: id }, { Category, Array(Product) })
                MATCH (product:Product)-[:IN_CATEGORY]->(category:Category)
                WHERE category.id = $id
                RETURN category, collect(product)
                LIMIT 1
              CYPHER
              result = results.first
              category, products = result

              category.should be_a Category
              products.should be_a Array(Product)
              category.id.should eq UUID.new(id)
              category.name.should eq "Stuff"
              product_names = products.map(&.name)
              product_names.includes?("Thing 1").should eq true
              product_names.includes?("Thing 2").should eq true

              txn.rollback
            end
          end

          it "supports union types for mapped nodes" do
            connection.transaction do |txn|
              id = connection.exec_cast(<<-CYPHER, { UUID })
                CREATE (category : Category {
                  id: randomUUID(),
                  name: "Stuff"
                })

                CREATE (product : Product {
                  id: randomUUID(),
                  name: "My Product"
                })

                CREATE (something_else : SomethingElse {
                  id: randomUUID(),
                  name: "Foo",
                  foo: 32
                })

                CREATE (product)-[:IN_CATEGORY]->(category)
                CREATE (something_else)-[:IN_CATEGORY]->(category)

                RETURN category.id
              CYPHER
                .first
                .first

              # pp connection.execute(<<-CYPHER, id: id.to_s)
              #   MATCH (thing)-[:IN_CATEGORY]->(category : Category { id: $id })
              #   RETURN thing
              # CYPHER

              results = connection.exec_cast(<<-CYPHER, Map { "id" => id.to_s }, { Product | SomethingElse })
                MATCH (thing)-[:IN_CATEGORY]->(category : Category { id: $id })
                RETURN thing
                ORDER BY labels(thing)
              CYPHER
                .map(&.first)

              results.should be_a Array(Product | SomethingElse)
              results[0].should be_a Product
              results[1].should be_a SomethingElse

              txn.rollback
            end
          end

          it "supports unions of primitive types" do
            connection.transaction do |txn|
              connection.exec_cast("RETURN 42", { Int32 | Int64 })
                .first
                .first
                .should be_a Int64

              connection.exec_cast("UNWIND [1, 'hello'] AS value RETURN value", { Int32 | String })
                .map(&.first) # Unwrap the tuples
                .should eq [1, "hello"]

              txn.rollback
            end
          end

          it "deserializes arrays of properties into mapped nodes" do
            connection.transaction do |txn|
              connection.exec_cast <<-CYPHER,
                CREATE (zone:Zone { polygon: $polygon })
                RETURN zone
              CYPHER
                {
                  polygon: [
                    LatLng.new(39, -76),
                    LatLng.new(40, -76),
                    LatLng.new(40, -75),
                    LatLng.new(39, -75),
                  ],
                },
                { Zone }

              txn.rollback
            end
          end

          it "deserializes and passes to a block" do
            connection.transaction do |txn|
              values = Array(Int8).new

              connection.exec_cast <<-CYPHER, { max: 100 }, { Int8 } do |(value)|
                UNWIND range(1, $max) AS index
                RETURN index
              CYPHER
                values << value
              end

              values.should eq (1..100).to_a
            end
          end

          it "deserializes nilable values" do
            connection.transaction do |txn|
              connection.exec_cast_single <<-CYPHER, Map.new, {Subscription, Subscription}
                CREATE (affiliated:Subscription {
                  id: randomUUID(),
                  amount_cents: 10000,
                  last_billed_at: datetime({ year: 2019, month: 1, day: 1 }),
                  next_bill_at: datetime(),
                  frequency: duration({ months: 1 })
                })-[:FOR_USER]->(:User {
                  id: randomUUID(),
                  name: 'Jamie'
                })

                CREATE (unaffiliated:Subscription {
                  id: randomUUID(),
                  amount_cents: 5000,
                  last_billed_at: datetime({ year: 2019, month: 1, day: 1 }),
                  next_bill_at: datetime(),
                  frequency: duration({ months: 1 })
                })

                RETURN affiliated, unaffiliated
              CYPHER

              query = <<-CYPHER
                MATCH (subscription:Subscription)
                OPTIONAL MATCH (subscription)-[:FOR_USER]->(user:User)
                RETURN subscription, user
              CYPHER

              connection.exec_cast query, Map{"id" => "123"}, {Subscription, User?} do |(subscription, user)|
                if subscription.amount_cents == 100_00
                  pp user
                  user.should be_a User
                elsif subscription.amount_cents == 50_00
                  pp user
                  user.should eq nil
                end
              end

              txn.rollback
            end
          end
        end
      end
    end
  end
end
