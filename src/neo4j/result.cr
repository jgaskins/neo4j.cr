require "./exceptions"

module Neo4j
  class Result
    include Enumerable(List)

    getter type : Success | Ignored
    getter data : Array(List)

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
