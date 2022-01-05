require "uuid"
require "uri"

require "../pack_stream/token"
require "../pack_stream/unpacker"
require "../exceptions"

def Nil.from_bolt(io)
  Neo4j::PackStream::Unpacker.new(io).read_nil
end

{% for size in %w(8 16 32 64) %}
  def Int{{size.id}}.from_bolt(io)
    Neo4j::PackStream::Unpacker.new(io).read_int.to_i{{size.id}}
  end
{% end %}

def Float64.from_bolt(io)
  Neo4j::PackStream::Unpacker.new(io).read_float
end

def String.from_bolt(io)
  Neo4j::PackStream::Unpacker.new(io).read_string
end

def Bool.from_bolt(io)
  Neo4j::PackStream::Unpacker.new(io).read_bool
end

def Time.from_bolt(io) : Time
  Neo4j::PackStream::Unpacker.new(io).read_structure.as(Time)
end

def URI.from_bolt(io) : URI
  URI.parse String.from_bolt io
end

def Array.from_bolt(io)
  unpacker = Neo4j::PackStream::Unpacker.new(io)

  token = unpacker.next_token
  unpacker.check Neo4j::PackStream::Token::Type::Array
  new(token.size.to_i32) { T.from_bolt(io) }
end

def Hash.from_bolt(io)
  Neo4j::PackStream::Unpacker.new(io).read_hash.as(Hash(K, V))
end

module Neo4j
  def Point2D.from_bolt(io)
    PackStream::Unpacker.new(io).read_structure.as(Point2D)
  end

  def LatLng.from_bolt(io)
    PackStream::Unpacker.new(io).read_structure.as(LatLng)
  end

  def Point3D.from_bolt(io)
    PackStream::Unpacker.new(io).read_structure.as(Point3D)
  end

  def Node.from_bolt(io)
    PackStream::Unpacker.new(io).read_structure.as(Node)
  end

  def Relationship.from_bolt(io)
    PackStream::Unpacker.new(io).read_structure.as(Relationship)
  end

  def Duration.from_bolt(io)
    PackStream::Unpacker.new(io).read_structure.as(self)
  end
end

def UUID.from_bolt(io)
  new(String.from_bolt(io))
end

struct Tuple
  def from_bolt(io)
    self.class.from_bolt io
  end

  def self.from_bolt(io)
    {% begin %}
      {
        {% for type in T.map(&.instance) %}
          {{type}}.from_bolt(io),
        {% end %}
      }
    {% end %}
  end
end

def Union.from_bolt(io) : self
  {% begin %}
    unpacker = ::Neo4j::PackStream::Unpacker.new(io)
    unpacker.prefetch_token
    token = unpacker.token

    {% non_primitive_types = T.reject { |type| [Nil, Bool, Int8, Int16, Int32, Int64, Float64, String, Time].includes? type } %}

    {% if T.includes? Bool %}
      return unpacker.read_bool if token.type.bool?
    {% end %}
    {% if T.any? { |type| type < Int } %}
      if token.type.int?
        {% largest_int_size = T
             .select { |t| t.stringify.includes? "Int" }
             .map { |t| t.stringify.gsub(/Int/, "").to_i }
             .sort
             .last %}
        return unpacker.read_int.to_i{{largest_int_size}}
      end
    {% end %}
    {% if T.includes? Float64 %}
      return unpacker.read_float if token.type.float?
    {% end %}
    {% if T.includes? String %}
      return unpacker.read_string if token.type.string?
    {% end %}
    {% if T.includes? Time %}
      return unpacker.read_value.as(Time) if token.type.structure?
    {% end %}
    {% if T.includes? Nil %}
      return nil if token.type.null?
    {% end %}

    {% if non_primitive_types.empty? %}
      raise ::Neo4j::UnknownType.new("Don't know how to cast #{unpacker.read_value.inspect} into #{{{T.join(" | ")}}}")
    {% else %}
      value = unpacker.read_value
      {% for type in non_primitive_types %}
        {% if type.resolve.ancestors.includes? Neo4j::Serializable::Node %}
          node = value.as(::Neo4j::Node)
          return {{type}}.new(node) if node.labels.includes?({{type.stringify}})
          {% if node_labels = type.annotation(::Neo4j::NodeLabels) %}
            if ({{node_labels.args.first}} & node.labels).any?
              return {{type}}.new(node)
            end
          {% end %}
        {% elsif type.resolve.ancestors.includes? Neo4j::Serializable::Relationship %}
          rel = value.as(::Neo4j::Relationship)
          return {{type}}.new(rel) if rel.type == {{type.stringify}}
          {% if rel_type = type.annotation(::Neo4j::RelationshipType) %}
            if {{rel_type.args.first}} == rel.type
              return {{type}}.new(rel)
            end
          {% end %}
        {% end %}
      {% end %}
      raise ::Neo4j::UnknownType.new("Don't know how to cast #{value.inspect} into #{{{T.join(" | ")}}}")
    {% end %}
  {% end %}
end

class Object
  def to_bolt_params : Neo4j::Value
    as Neo4j::Value
  end
end

class Hash
  def to_bolt_params : Neo4j::Value
    each_with_object(Neo4j::Map.new) do |(key, value), hash|
      hash[key] = value.to_bolt_params.as(Neo4j::Value)
    end.as(Neo4j::Value)
  end
end

class Array
  def to_bolt_params : Neo4j::Value
    map(&.to_bolt_params.as(Neo4j::Value)).as Neo4j::Value
  end
end

struct UUID
  def to_bolt_params : Neo4j::Value
    to_s.to_bolt_params
  end
end
