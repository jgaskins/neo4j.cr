module Neo4j
  struct Node
    getter(
      id : Int64,
      labels : Array(String),
      properties : Map,
    )

    def initialize(@id, @labels, @properties)
    end
  end

  struct Relationship
    getter(
      id : Int64,
      start : Int64,
      end : Int64,
      type : String,
      properties : Map,
    )

    def initialize(@id, @start, @end, @type, @properties)
    end
  end

  struct UnboundRelationship
    getter(
      id : Int64,
      type : String,
      properties : Map,
    )

    def initialize(@id, @type, @properties)
    end
  end

  struct Path
    include Enumerable(Tuple(Node, UnboundRelationship, Node))

    getter(
      nodes : Array(Node),
      relationships : Array(UnboundRelationship),
      sequence : Array(Int8),
    )

    def initialize(@nodes, @relationships, @sequence)
    end

    def each
      (sequence.size / 2).times do |index|
        yield({ nodes[index], relationships[index.abs - 1], nodes[index + 1] })
      end
    end
  end

  struct Success
    getter attrs : Map

    def initialize(@attrs)
    end

    def fields : Array(String)
      if attrs["fields"]
        attrs["fields"]
          .as(Array)
          .map(&.as(String))
      else
        [] of String
      end
    end
  end

  struct Failure
    getter attrs : Map

    def initialize(@attrs)
    end
  end

  struct Ignored
  end

  struct Point2D
    getter x, y, type

    def initialize(@x : Float64, @y : Float64, @type : Int16 = 7203_i16)
    end
  end

  struct Point3D
    getter x, y, z, type

    def initialize(@x : Float64, @y : Float64, @z : Float64, @type : Int16 = 9157_i16)
    end
  end

  struct LatLng
    getter latitude, longitude, type

    def initialize(@latitude : Float64, @longitude : Float64, @type = 4326_i16)
    end
  end

  alias Value =
    Nil |
    Bool |
    String |
    Int8 |
    Int16 |
    Int32 |
    Int64 |
    Float64 |
    Time |
    Point2D |
    Point3D |
    LatLng |
    Node |
    Relationship |
    UnboundRelationship |
    Path |
    Array(Value) |
    Hash(String, Value)

  alias List = Array(Value)
  alias Map = Hash(String, Value)

  alias Response = Success | Failure | Ignored

  alias Type = Value | Response
end
