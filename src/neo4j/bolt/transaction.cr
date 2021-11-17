require "../exceptions"

module Neo4j
  module Bolt
    class Transaction
      getter connection
      getter? rolled_back = false

      delegate execute, exec_cast, exec_cast_scalar, to: connection

      def initialize(@connection : Neo4j::Bolt::Connection)
      end

      def rollback
        @connection.rollback
        @rolled_back = true
      end

      struct RolledBack
      end
    end
  end
end
