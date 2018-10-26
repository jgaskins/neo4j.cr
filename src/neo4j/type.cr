module Neo4j
  struct Node
    @properties : Hash(String, Type)
    getter :properties
    getter(
      id : Int32,
      labels : Array(String),
    )

    def initialize(@id, @labels, @properties)
    end
  end

  struct Relationship
    @properties : Hash(String, Type)
    getter :properties

    getter(
      id : Int32,
      start : Int32,
      end : Int32,
      type : String,
    )

    def initialize(@id, @start, @end, @type, @properties)
    end
  end

  struct UnboundRelationship
    getter(
      id : Int32,
      type : String,
    )

    @properties : Hash(String, Type)
    getter :properties

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
    getter attrs : Hash(String, Type)

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
    getter attrs : Hash(String, Type)

    def initialize(@attrs)
    end
  end

  struct Ignored
  end

  alias Type = Nil |
    Bool |
    String |
    Int8 |
    Int16 |
    Int32 |
    Int64 |
    Float64 |
    Array(Type) |
    Hash(String, Type) |
    Node |
    Relationship |
    UnboundRelationship |
    Path |
    Success |
    Failure |
    Ignored
end
