require "uri"
require "socket"
require "openssl"

require "./neo4j/pack_stream"
require "./neo4j/mapping"
require "./neo4j/bolt/connection"

module Neo4j
  class Exception < ::Exception
  end

  class QueryException < Exception
    getter code : String

    def initialize(message, @code)
      super "#{message} [#{code}]"
    end
  end

  class UnknownResult < Exception
    def initialize(@message)
    end
  end

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
