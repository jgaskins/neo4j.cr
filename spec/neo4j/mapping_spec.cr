require "../spec_helper"

require "../../src/neo4j/mapping"

struct MappingNodeExample
  Neo4j.map_node(
    name: String,
    created_at: Time,
    number: Int64,
    nilable_value: { type: String, nilable: true },
    nilable_question_mark: Int::Signed?,
    nonexistent_on_node: Int64?,
    int_with_default: { type: Int32, default: 0 },
    string_with_default: { type: String, default: "hi" },
    string_with_other_key: { type: String, key: "stringWithOtherKey" },
    datestamp: { type: Time, converter: DateStampConversion, key: "date" },
    optional_datestamp: { type: Time?, converter: DateStampConversion, key: "optional_date" },
  )

  module DateStampConversion
    def self.deserialize(value)
      raise Exception.new("Cannot convert #{value.inspect} to a Time")
    end

    def self.deserialize(value : String)
      year, month, day = value.split("-", 3).map(&.to_i)
      Time.utc(year, month, day)
    end
  end
end

struct MappingRelationshipExample
  Neo4j.map_relationship(
    role: String,
    added_at: Time,
    number: Int32,
    nilable_value: { type: String, nilable: true },
    int_with_default: { type: Int64, default: 0 },
    string_with_default: { type: String, default: "hello" },
  )
end

module Neo4j
  describe "mapping" do
    it "maps nodes to models" do
      model = MappingNodeExample.new(Node.new(
        id: 123_i64,
        labels: ["Foo", "Bar"],
        properties: Map {
          "name" => "Jamie",
          "created_at" => Time.utc(2015, 4, 20, 16, 20, 31).to_unix,
          "number" => 42_i8,
          "nilable_value" => nil,
          "nilable_question_mark" => nil,
          "stringWithOtherKey" => "value",
          "date" => "2019-01-23",
        },
      ))

      # Node properties, the important stuff

      model.name.should eq "Jamie"
      model.created_at.should eq Time.utc(2015, 4, 20, 16, 20, 31)
      model.number.should eq 42
      model.nilable_value.should eq nil
      model.nilable_question_mark.should eq nil
      model.nonexistent_on_node.should eq nil
      model.int_with_default.should eq 0
      model.string_with_default.should eq "hi"
      model.string_with_other_key.should eq "value"
      model.datestamp.should eq Time.utc(2019, 1, 23)
      model.optional_datestamp.should eq nil

      # Node metadata, the subtly important stuff

      model.node_id.should eq 123
      model.node_labels.should eq %w(Foo Bar)
    end

    it "maps relationships to models" do
      model = MappingRelationshipExample.new(Relationship.new(
        id: 123_i64,
        start: 456_i64,
        end: 789_i64,
        type: "FOO_BAR",
        properties: Map {
          "role" => "user",
          "added_at" => Time.utc(2015, 4, 20, 16, 20, 31).to_unix,
          "number" => 42,
          "nilable_value" => "not nil this time",
        },
      ))

      # Properties

      model.role.should eq "user"
      model.added_at.should eq Time.utc(2015, 4, 20, 16, 20, 31)
      model.number.should eq 42
      model.nilable_value.should eq "not nil this time"
      model.int_with_default.should eq 0
      model.string_with_default.should eq "hello"

      # Relationship metadata

      model.relationship_id.should eq 123
      model.node_start.should eq 456
      model.node_end.should eq 789
      model.relationship_type.should eq "FOO_BAR"
    end
  end
end
