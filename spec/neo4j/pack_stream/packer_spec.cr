require "../../spec_helper"

require "../../../src/neo4j/pack_stream"
require "../../../src/neo4j/type"

module Neo4j
  module PackStream
    describe Packer do
      it "serializes hashes over 4 bits in length" do
        hash = {
          key_00: "value",
          key_01: "value",
          key_02: "value",
          key_03: "value",
          key_04: "value",
          key_05: "value",
          key_06: "value",
          key_07: "value",
          key_08: "value",
          key_09: "value",
          key_0A: "value",
          key_0B: "value",
          key_0C: "value",
          key_0D: "value",
          key_0E: "value",
          key_0F: "value",
          key_10: "value",
        }.to_h.transform_keys(&.to_s)

        unpack(pack(hash)).should eq hash
      end

      {
        0_i8,
        -1_i8,
        -17_i8,
        1_i8,
        Int8::MAX,
        Int8::MIN,

        0_i16,
        -1_i16,
        1_i16,
        Int8::MIN.to_i16 - 1,
        Int8::MAX.to_i16 + 1,
        Int16::MIN,
        Int16::MAX,

        0_i32,
        -1_i32,
        1_i32,
        Int16::MIN.to_i32 - 1,
        Int16::MAX.to_i32 + 1,
        Int32::MIN,
        Int32::MAX,

        0__i64,
        -1_i64,
        1__i64,
        Int32::MIN.to_i64 - 1,
        Int32::MAX.to_i64 + 1,
        Int64::MIN,
        Int64::MAX,
      }.each do |int|
        it "serializes #{int}" do
          unpack(pack(int)).should eq int
        end
      end

      {
        Point2D.new(x: 1, y: 2),
        Point3D.new(x: 1, y: 2, z: 3),
        LatLng.new(latitude: 12.34, longitude: 56.78),
        ::Time.local(
          year: 2019,
          month: 2,
          day: 16,
          hour: 10,
          minute: 19,
          second: 35,
          nanosecond: 123456789,
          location: ::Time::Location.load("America/New_York"),
        ),
        Node.new(
          id: 123_i64,
          labels: ["Foo"],
          properties: Map {
            "foo" => "bar",
            "answer" => 42,
            "contrived" => true,
          },
        ),
        Relationship.new(
          id: 123_i64,
          start: 456_i64,
          end: 789_i64,
          type: "OMG_LOL",
          properties: Map {
            "one" => 1,
          },
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
