require "../../../src/neo4j/pack_stream/packer"
require "../../../src/neo4j/type"

module Neo4j
  module PackStream
    describe Packer do
      {
        Point2D.new(x: 1, y: 2),
        Point3D.new(x: 1, y: 2, z: 3),
        LatLng.new(latitude: 12.34, longitude: 56.78),
        Node.new(
          id: 123,
          labels: ["Foo"],
          properties: {
            "foo" => "bar",
            "answer" => 42,
            "contrived" => true,
          } of String => Type,
        ),
        Relationship.new(
          id: 123,
          start: 456,
          end: 789,
          type: "OMG_LOL",
          properties: {
            "one" => 1,
          } of String => Type,
        ),
      }.each do |value|
        it "serializes and deserializes #{value.class}" do
          io = IO::Memory.new
          packer = Packer.new(io)
          unpacker = Unpacker.new(io)

          packer.write value
          io.rewind
          unpacker.read_value.should eq value
        end
      end
    end
  end
end
