require "../exceptions"

module Neo4j
  module Bolt
    struct Transaction
      getter connection

      def initialize(@connection : Neo4j::Bolt::Connection)
      end

      def execute(_query, **parameters)
        connection.execute _query, **parameters
      end

      def execute(query, parameters : Hash(String, Type))
        connection.execute query, parameters
      end

      def rollback
        raise RollbackException.new
      end
    end
  end
end
