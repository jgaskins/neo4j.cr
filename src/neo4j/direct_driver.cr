require "./connection_pool"
require "./session"
require "./driver"

module Neo4j
  class DirectDriver < Driver
    def initialize(@uri : URI, @ssl : Bool = true)
      @connection_pool = ConnectionPool.new(
        initial_pool_size: 0,
        max_pool_size: 0, # 0 == unlimited
        max_idle_pool_size: 20,
        checkout_timeout: 5.seconds,
        retry_attempts: 3,
        retry_delay: 200.milliseconds,
      ) do
        Bolt::Connection.new(uri, ssl: ssl)
      end
    end

    def initialize(url : String, ssl : Bool = false)
      initialize URI.parse(url), ssl
    end

    def session
      session = Session.new self
      yield session
    ensure
      session.close if session
    end

    def connection
      connection = @connection_pool.checkout
      yield connection
    ensure
      @connection_pool.release connection if connection
    end

    class Session < ::Neo4j::Session
      @closed = false
      @connection : Bolt::Connection? = nil

      delegate execute, exec_cast, exec_cast_scalar, to: connection

      def initialize(@driver : DirectDriver)
      end

      def write_transaction(retries = 0)
        loop do
          return transaction { |txn| yield txn }
        rescue ex
          if retries <= 0
            raise ex
          else
            retries -= 1
          end
        end
      end

      def read_transaction(retries = 0)
        loop do
          return transaction { |txn| yield txn }
        rescue ex
          if retries <= 0
            raise ex
          else
            retries -= 1
          end
        end
      end

      # def execute(query : String, **params) forall T
      #   transaction(&.execute(query, params))
      # end

      # def exec_cast(query : String, as types : Tuple(*T), **params) forall T
      #   transaction(&.exec_cast(query, params, types) { |row| yield row })
      # end
      # def read_query(query : String, as types : Tuple(*T), **params) forall T
      #   read_transaction(&.
      # end
      # def write_query(query : String, as types : Tuple(*T), **params) forall T

      # end

      def transaction
        connection do |connection|
          connection.transaction do |txn|
            yield txn
          end
        end
      end

      def exec_cast(query : String, as types, **params)
        transaction(&.exec_cast(query, params, types))
      end

      def connection : Bolt::Connection
        @connection ||= @driver.@connection_pool.checkout
      end

      def connection
        @driver.connection { |c| yield c }
      end

      def close : Nil
        if connection = @connection
          @driver.@connection_pool.release connection
        end
        @closed = true
      end

      def closed? : Bool
        @closed
      end

      def finalize
        close
      end
    end
  end
end
