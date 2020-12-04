require "../spec_helper"
require "uuid"
require "crypto/bcrypt/password"

require "../../src/neo4j/serializable"
require "../../src/neo4j/pack_stream"

# BCrypt::Password is *way* easier to type than Crypto::Bcrypt::Password
alias BCrypt = Crypto::Bcrypt

class BCrypt::Password
  def self.from_bolt(unpacker)
    new unpacker.read_string
  end
end

class SerializableNode
  include Neo4j::Serializable::Node

  getter id : UUID
  getter name : String
  @[Neo4j::Field(key: "encrypted_password", converter: BCrypt::Password)]
  getter password : BCrypt::Password
  getter created_at : Time = Time.utc
  getter updated_at : Time = Time.utc
end

@[Neo4j::NodeLabels(%w[Yep Nope])]
struct AnotherSerializableNode
  include Neo4j::Serializable::Node

  getter id : UUID
end

class NodeWithNumbers
  include Neo4j::Serializable::Node

  getter tiny_int : Int8?
  getter small_int : Int16?
  getter int : Int32 = 0
  getter big_int : Int64?
end

@[Neo4j::RelationshipType("MEMBER_OF")]
struct Membership
  include Neo4j::Serializable::Relationship

  getter role : String
  getter created_at : Time = Time.utc
  getter updated_at : Time = Time.utc
end

module Neo4j
  describe Serializable::Node do
    it "deserializes from Bolt" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      uuid = UUID.random
      node = Node.new(1234_i64, %w[MyLabel], Map {
        "id" => uuid.to_s,
        "name" => "Jamie",
        "encrypted_password" => BCrypt::Password.create("password", cost: 4).to_s,
        "created_at" => Time.utc,
        # updated_at is omitted so it uses the default value
      })
      packer.write node

      s_node = SerializableNode.from_bolt io.rewind

      s_node.id.should eq uuid
      s_node.name.should eq "Jamie"
      s_node.password.verify("password").should eq true # conversion to BCrypt::Password check
      s_node.created_at.should be_a Time
      s_node.updated_at.should be_a Time
    end

    it "deserializes unions by class name" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      uuid = UUID.random
      node = Node.new(1234_i64, %w[SerializableNode], Map {
        "id" => uuid.to_s,
        "name" => "Jamie",
        "encrypted_password" => BCrypt::Password.create("password", cost: 4).to_s,
        "created_at" => Time.utc,
        # updated_at is omitted so it uses the default value
      })
      packer.write node

      s_node = (SerializableNode | AnotherSerializableNode).from_bolt io.rewind

      s_node.should be_a SerializableNode
      s_node.id.should eq uuid
    end

    it "deserializes unions by NodeLabels annotation" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      uuid = UUID.random
      node = Node.new(1234_i64, %w[Yep], Map { "id" => uuid.to_s })
      packer.write node

      s_node = (SerializableNode | AnotherSerializableNode).from_bolt io.rewind

      s_node.should be_a AnotherSerializableNode
      s_node.id.should eq uuid
    end

    it "raises an error if it cannot find a suitable match by class name or annotation" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      uuid = UUID.random
      node = Node.new(1234_i64, %w[Yep], Map { "id" => uuid.to_s })
      packer.write node

      s_node = (SerializableNode | AnotherSerializableNode).from_bolt io.rewind

      s_node.should be_a AnotherSerializableNode
      s_node.id.should eq uuid
    end

    it "casts ints to correct size" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      uuid = UUID.random
      node = Node.new(properties: Map { "small_int" => 420_i16, "int" => 42, "big_int" => 8_i8 })
      packer.write node
      # pp io.to_slice.map(&.to_s(16))

      s_node = NodeWithNumbers.from_bolt io.rewind

      s_node.big_int.should eq 8_i64
    end
  end

  describe Serializable::Relationship do
    it "deserializes from Bolt" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      rel = Relationship.new(
        id: 1234_i64,
        type: "MEMBER_OF",
        properties: Map {
          "role" => "admin",
          "created_at" => Time.utc,
          # updated_at is omitted so it uses the default value
        },
        start: 0_i64,
        end: 1_i64,
      )
      packer.write rel

      s_rel = Membership.from_bolt io.rewind

      s_rel.role.should eq "admin"
      s_rel.created_at.should be_a Time
      s_rel.updated_at.should be_a Time
    end

    it "deserializes unions by RelationshipType annotation" do
      io = IO::Memory.new
      packer = PackStream::Packer.new(io)
      rel = Relationship.new(
        id: 1234_i64,
        type: "MEMBER_OF",
        start: 0_i64,
        end: 1_i64,
        properties: Map { "role" => "admin" },
      )
      packer.write rel

      s_rel = (Membership | Nil).from_bolt(io.rewind).as(Membership)

      s_rel.should be_a Membership
      s_rel.relationship_id.should eq 1234
      s_rel.relationship_type.should eq "MEMBER_OF"
      s_rel.node_start.should eq 0
      s_rel.role.should eq "admin"
    end

    # it "raises an error if it cannot find a suitable match by class name or annotation" do
    #   io = IO::Memory.new
    #   packer = PackStream::Packer.new(io)
    #   uuid = UUID.random
    #   node = Node.new(1234_i64, %w[Yep], Map { "id" => uuid.to_s })
    #   packer.write node

    #   s_node = (SerializableNode | AnotherSerializableNode).from_bolt io.rewind

    #   s_node.should be_a AnotherSerializableNode
    #   s_node.id.should eq uuid
    # end

    # it "casts ints to correct size" do
    #   io = IO::Memory.new
    #   packer = PackStream::Packer.new(io)
    #   uuid = UUID.random
    #   node = Node.new(properties: Map { "small_int" => 420_i16, "int" => 42, "big_int" => 8_i8 })
    #   packer.write node
    #   # pp io.to_slice.map(&.to_s(16))

    #   s_node = NodeWithNumbers.from_bolt io.rewind

    #   s_node.big_int.should eq 8_i64
    # end
  end
end
