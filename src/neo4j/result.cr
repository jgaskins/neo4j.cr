require "./exceptions"

module Neo4j
  class Result
    include Enumerable(Array(Type))

    getter type : Success | Ignored
    getter data : Array(Array(Type))

    def initialize(@type, @data)
    end

    def each
      @data.each do |row|
        yield row
      end
    end

    def size
      @data.size
    end

    def fields
      @type.fields
    end
  end
end
