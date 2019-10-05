require "uuid"

require "../pack_stream/token"
require "../pack_stream/unpacker"
require "../exceptions"

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

def Array.from_bolt(io)
  unpacker = Neo4j::PackStream::Unpacker.new(io)

  token = unpacker.next_token
  unpacker.check Neo4j::PackStream::Token::Type::Array
  new(token.size.to_i32) { T.from_bolt(io) }
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
end

def UUID.from_bolt(io)
  new(String.from_bolt(io))
end

struct Tuple
  def from_bolt(io)
    {% begin %}
      {
        {% for type in T.map(&.stringify.gsub(/\.class$/, "").id) %}
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

    {% non_primitive_types = T.reject { |type| [Nil, Bool, Int8, Int16, Int32, Int64, Float64, String].includes? type } %}

    {% if T.includes? Bool %}
      return unpacker.read_bool if token.type.bool?
    {% end %}
    {% if T.any? { |type| type < Int } %}
      if token.type.int?
        {% largest_int_size = T
          .select { |t| t.stringify.includes? "Int" }
          .map { |t| t.stringify.gsub(/Int/, "").to_i }
          .sort
          .last
        %}
        return unpacker.read_int.to_i{{largest_int_size}}
      end
    {% end %}
    {% if T.includes? Float64 %}
      return unpacker.read_float if token.type.float?
    {% end %}
    {% if T.includes? String %}
      return unpacker.read_string if token.type.string?
    {% end %}

    {% if non_primitive_types.empty? %}
      raise ::Neo4j::UnknownType.new("Don't know how to cast #{unpacker.read_value.inspect} into #{{{T.join(" | ")}}}")
    {% else %}
      structure = unpacker.read_value
      {% for type in non_primitive_types %}
        case structure
        {% if T.includes? Nil %}
          when Nil
            return nil
        {% end %}
        when ::Neo4j::Node
          return {{type}}.new structure if structure.labels.includes? {{type.stringify}}
        end
      {% end %}
      raise ::Neo4j::UnknownType.new("Don't know how to cast #{structure.inspect} into #{{{T}}.inspect}")
    {% end %}
  {% end %}
end
