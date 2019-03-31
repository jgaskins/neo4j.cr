require "./type"
require "./exceptions"

module Neo4j
  macro map_relationship(**__properties__)
    ::Neo4j.map_relationship({{__properties__}})
  end

  macro map_relationship(__properties__)
    getter relationship_id : Int32
    getter node_start : Int32
    getter node_end : Int32
    getter relationship_type : String

    ::Neo4j.map_props({{__properties__}}, ::Neo4j::Relationship)
  end

  macro map_node(**__properties__)
    ::Neo4j.map_node({{__properties__}})
  end

  macro map_node(__properties__)
    getter node_id : Int32
    getter node_labels : Array(String)

    ::Neo4j.map_props({{__properties__}}, ::Neo4j::Node)
  end

  macro map_props(__properties__, type)
    {% for key, value in __properties__ %}
      {% unless value.is_a?(HashLiteral) || value.is_a?(NamedTupleLiteral) %}
        {% __properties__[key] = { type: value } %}
      {% end %}
      {% __properties__[key][:key_id] = key.id.gsub(/\?$/, "") %}
      {% if value.is_a?(Generic) && value.type_vars.any?(&.resolve.nilable?) %}
        {% __properties__[key][:nilable] = true %}
        {% __properties__[key][:optional] = true %}
      {% end %}
    {% end %}

    {% for key, value in __properties__ %}
      @{{value[:key_id]}} : {{value[:type]}}{{ (value[:nilable] ? "?" : "").id }}

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
        %property_value = %node.properties.fetch({{key.stringify}}) do |key|
          {% if value[:default] %}
            {{value[:default]}}
          {% elsif value[:nilable] %}
            nil
          {% else %}
            {% if type.resolve == ::Neo4j::Node %}
              raise PropertyMissing.new("Node with id #{@node_id} and labels #{@node_labels.inspect} is missing property #{key}")
            {% elsif type.resolve == ::Neo4j::Relationship %}
              raise PropertyMissing.new("Relationship with id #{@relationship_id} and type #{@relationship_type.inspect} is missing property #{key}")
            {% end %}
          {% end %}
        end

        {% if value[:type].stringify == "UUID" %}
          @{{value[:key_id]}} = UUID.new(%property_value.as(String))
        {% elsif value[:type].stringify == "Time" %}
          case %property_value
          when String
            @{{value[:key_id]}} = Time.parse_iso8601(%property_value)
          when Int
            @{{value[:key_id]}} = Time.unix(%property_value)
          when Time
            @{{value[:key_id]}} = %property_value
          {% if value[:nilable] %}
            when Nil
              @{{value[:key_id]}} = %property_value
          {% end %}
          else
            raise ArgumentError.new("Property #{{{key.id}}} must be a String or Int value to cast into a Time")
          end
        {% elsif value[:type].stringify.includes? "UInt" %}
          {% int_bit_size = value[:type].stringify.gsub(/\D+/, "") %}
          @{{key.id}} = %property_value.as(Int).to_u{{int_bit_size.id}}{% if value[:nilable] %} if %property_value {% end %}
        {% elsif value[:type].stringify.includes? "Int" %}
          {% int_bit_size = value[:type].stringify.gsub(/\D+/, "") %}
          @{{key.id}} = %property_value.as(Int).to_i{{int_bit_size.id}}{% if value[:nilable] %} if %property_value {% end %}
        {% else %}
          @{{key.id}} = %property_value.as({{value[:type]}}{{(value[:nilable] && !value[:optional] ? "?" : "").id}})
        {% end %}
      {% end %}
    end
  end
end
