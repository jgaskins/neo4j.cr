require "./connection_pool"

module Neo4j
  class DirectDriver
    def initialize(@uri : URI, @ssl : Bool = true)
      puts "Initializing connection pool for #{uri}..."
      @connection_pool = ConnectionPool.new(
        initial_pool_size: 1,
        max_pool_size: 0, # 0 == unlimited
        max_idle_pool_size: 10,
        checkout_timeout: 5.seconds,
        retry_attempts: 3,
        retry_delay: 200.milliseconds,
      ) do
        puts "Spinning up a connection to #{uri}..."
        Bolt::Connection.new(uri, ssl: ssl)
      end
    end

    def initialize(url : String, ssl : Bool = false)
      initialize URI.parse(url), ssl
    end

    def write_transaction
      transaction { |txn| yield txn }
    end

    def read_transaction
      transaction { |txn| yield txn }
    end

    def transaction
      connection do |connection|
        connection.transaction do |txn|
          yield txn
        end
      end
    end

    private def connection
      connection = @connection_pool.checkout
      yield connection
    ensure
      @connection_pool.release connection if connection
    end
  end
end
