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
