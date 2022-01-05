module Neo4j
  class Exception < ::Exception
  end

  class UnknownResult < Exception
  end

  class UnknownType < Exception
  end

  class QueryException < Exception
    getter code : String
    @query : String?

    def initialize(message, @code, @query = "")
      super "#{message} [#{code}]#{" - Query: #{@query}" if @query}"
    end
  end

  class IndexAlreadyExists < QueryException
  end

  class ConstraintValidationFailed < QueryException
  end

  class EquivalentSchemaRuleAlreadyExists < QueryException
  end

  class ProcedureCallFailed < QueryException
  end

  class ParameterMissing < QueryException
  end

  class ArgumentError < QueryException
  end

  class SyntaxError < QueryException
  end

  class RollbackException < Exception
  end

  class NestedTransactionError < Exception
  end

  class PropertyMissing < Exception
  end

  class Timeout < Exception
  end
end
