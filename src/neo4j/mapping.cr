require "./type"
require "./exceptions"
require "./serializable"

module Neo4j
  module TimeConverter
    def self.serialize(time : Time) : Value
      time.to_bolt_params
    end

    def self.deserialize(value)
      raise ::ArgumentError.new("Property {{key.id}} must be a String or Int value to cast into a Time")
    end

    def self.deserialize(value : String) : Time
      Time.parse_iso8601 value
    end

    def self.deserialize(value : Int) : Time
      Time.unix value
    end

    def self.deserialize(value : Time) : Time
      value
    end
  end

  module UUIDConverter
    def self.deserialize(value)
      raise "Cannot convert #{value.inspect} to UUID"
    end

    def self.deserialize(value : String)
      UUID.new(value)
    end

    def self.serialize(value : UUID) : Neo4j::Value
      value.to_s.to_bolt_params
    end
  end

  macro map_relationship(**__properties__)
    ::Neo4j.map_relationship({{__properties__}})
  end

  module MappedRelationship
  end

  macro map_relationship(__properties__)
    getter relationship_id : Int64
    getter node_start : Int64
    getter node_end : Int64
    getter relationship_type : String

    include ::Neo4j::MappedRelationship

    ::Neo4j.map_props({{__properties__}}, ::Neo4j::Relationship)
  end

  macro map_node(**__properties__)
    ::Neo4j.map_node({{__properties__}})
  end

  macro map_node(__properties__)
    getter node_id : Int64
    getter node_labels : Array(String)

    include ::Neo4j::Serializable::Node

    ::Neo4j.map_props({{__properties__}}, ::Neo4j::Node)
  end

  macro map_props(__properties__, type)
    {% for key, value in __properties__ %}
      {% unless value.is_a?(HashLiteral) || value.is_a?(NamedTupleLiteral) %}
        {% __properties__[key] = {type: value} %}
      {% end %}
      {% __properties__[key][:key_id] = key.id.gsub(/\?$/, "") %}
      {% if __properties__[key][:type].is_a?(Generic) && __properties__[key][:type].type_vars.any?(&.resolve.nilable?) %}
        {% __properties__[key][:nilable] = true %}
        {% __properties__[key][:optional] = true %}
      {% end %}
    {% end %}

    {% for key, value in __properties__ %}
      {% if !value[:converter] %}
        {% if value[:type].stringify == "Time" %}
          {% value[:converter] = ::Neo4j::TimeConverter %}
        {% elsif value[:type].stringify == "UUID" %}
          {% value[:converter] = ::Neo4j::UUIDConverter %}
        {% else %}
          {% value[:converter] = nil %}
        {% end %}
      {% end %}
    {% end %}

    {% for key, value in __properties__ %}
      @{{value[:key_id]}} : {{value[:type]}}{{ (value[:nilable] ? "?" : "").id }}{{value[:default] ? " = #{value[:default]}".id : "".id}}

      {% if value[:getter] == nil || value[:getter] %}
        def {{key.id}} : {{value[:type]}}{{(value[:nilable] ? "?" : "").id}}
          @{{value[:key_id]}}
        end
      {% end %}

      {% if value[:presence] %}
        @{{value[:key_id]}}_present : Bool = false

        def {{value[:key_id]}}_present?
          @{{value[:key_id]}}_present
        end
      {% end %}
    {% end %}

    def initialize(%node : {{type}})
      {% if type.resolve == ::Neo4j::Node %}
        @node_id = %node.id
        @node_labels = %node.labels
      {% elsif type.resolve == ::Neo4j::Relationship %}
        @relationship_id = %node.id
        @node_start = %node.start
        @node_end = %node.end
        @relationship_type = %node.type
      {% end %}

      {% for key, value in __properties__ %}
        %property_value = %node.properties.fetch({{value[:key] || key.stringify}}) do |key|
          {% if value[:default] %}
            {{value[:default]}}
          {% elsif value[:nilable] %}
            nil
          {% else %}
            {% if type.resolve == ::Neo4j::Node %}
              raise ::Neo4j::PropertyMissing.new("Node with id #{@node_id} and labels #{@node_labels.inspect} is missing property #{key.inspect}")
            {% elsif type.resolve == ::Neo4j::Relationship %}
              raise ::Neo4j::PropertyMissing.new("Relationship with id #{@relationship_id} and type #{@relationship_type.inspect} is missing property #{key.inspect}")
            {% end %}
          {% end %}
        end

        {% if value[:type].stringify.includes? "UInt" %}
          {% int_bit_size = value[:type].stringify.gsub(/\D+/, "") %}
          @{{key.id}} = %property_value.as(Int).to_u{{int_bit_size.id}}{% if value[:nilable] %} if %property_value {% end %}
        {% elsif value[:type].stringify.includes? "Int" %}
          {% int_bit_size = value[:type].stringify.gsub(/\D+/, "") %}
          @{{key.id}} = %property_value.as(Int).to_i{{int_bit_size.id}}{% if value[:nilable] %} if %property_value {% end %}
        {% elsif value[:type].stringify.includes? "Array(" %}
          {% array_type = value[:type].type_vars %}
          @{{key.id}} = %property_value.as(Array).map { |value| value.as({{array_type.join(" | ").id}}) }
        {% elsif value[:converter] %}
          @{{value[:key_id]}} = {{value[:converter]}}.deserialize(%property_value) {% if value[:nilable] %} unless %property_value.nil? {% end %}
        {% else %}
          @{{key.id}} = %property_value.as({{value[:type]}}{{(value[:nilable] && !value[:optional] ? "?" : "").id}})
        {% end %}
      {% end %}
    end

    def to_bolt_params : Neo4j::Value
      ::Neo4j::Map {
        {% for var, type in __properties__ %}
          {{var.stringify}} => {{type[:converter] ? "#{type[:converter]}.serialize(#{var.id})".id : var.id}}.to_bolt_params,
        {% end %}
      }.as(Neo4j::Value)
    end
  end
end
