require "../spec_helper"

module Neo4j
  struct MappingNodeExample
    ::Neo4j.map_node(
      name: String,
      created_at: Time,
      number: Int32,
      nilable_value: { type: String, nilable: true },
    )
  end

  struct MappingRelationshipExample
    ::Neo4j.map_relationship(
      role: String,
      added_at: Time,
      number: Int32,
      nilable_value: { type: String, nilable: true },
    )
  end

  describe "mapping" do
    it "maps nodes to models" do
      model = MappingNodeExample.new(Node.new(
        id: 123,
        labels: ["Foo", "Bar"],
        properties: {
          "name" => "Jamie",
          "created_at" => Time.new(2015, 4, 20, 16, 20, 31).epoch,
          "number" => 42,
          "nilable_value" => nil,
        } of String => Type,
      ))

      # Node properties, the important stuff

      model.name.should eq "Jamie"
      model.created_at.should eq Time.new(2015, 4, 20, 16, 20, 31)
      model.number.should eq 42
      model.nilable_value.should eq nil

      # Node metadata, the subtly important stuff

      model.node_id.should eq 123
      model.node_labels.should eq %w(Foo Bar)
    end

    it "maps relationships to models" do
      model = MappingRelationshipExample.new(Relationship.new(
        id: 123,
        start: 456,
        end: 789,
        type: "FOO_BAR",
        properties: {
          "role" => "user",
          "added_at" => Time.new(2015, 4, 20, 16, 20, 31).epoch,
          "number" => 42,
          "nilable_value" => "not nil this time",
        } of String => Type,
      ))

      # Properties

      model.role.should eq "user"
      model.added_at.should eq Time.new(2015, 4, 20, 16, 20, 31)
      model.number.should eq 42
      model.nilable_value.should eq "not nil this time"

      # Relationship metadata

      model.relationship_id.should eq 123
      model.node_start.should eq 456
      model.node_end.should eq 789
      model.relationship_type.should eq "FOO_BAR"
    end
  end
end
