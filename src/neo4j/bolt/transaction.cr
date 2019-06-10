require "../exceptions"

module Neo4j
  module Bolt
    struct Transaction
      getter connection

      delegate execute, stream, exec_cast, to: connection

      def initialize(@connection : Neo4j::Bolt::Connection)
      end

      def rollback
        raise RollbackException.new
      end
    end
  end
end
