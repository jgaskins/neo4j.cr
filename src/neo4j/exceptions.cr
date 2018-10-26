module Neo4j
  class Exception < ::Exception
  end

  class UnknownResult < Exception
    def initialize(@message)
    end
  end

  class QueryException < Exception
    getter code : String

    def initialize(message, @code)
      super "#{message} [#{code}]"
    end
  end
end
