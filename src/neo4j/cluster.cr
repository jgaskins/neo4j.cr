require "uri"

require "./bolt/connection"
require "./connection_pool"
require "./session"

module Neo4j
  class Cluster
    @check_again_at : Time
    @write_servers : ConnectionPool
    @read_servers : ConnectionPool

    def initialize(entrypoint : URI, ssl = true, max_pool_size = 200)
      unless entrypoint.scheme == "bolt+routing"
        raise NotAClusterURI.new("The cluster entrypoint should be a 'bolt+routing' URI. Got: #{entrypoint}")
      end

      entrypoint = entrypoint.dup
      entrypoint.scheme = "bolt"

      start = Time.utc
      connection = Bolt::Connection.new(entrypoint, ssl: ssl)

      ttl, servers = connection.execute("call dbms.cluster.routing.getServers()").data.first

      @check_again_at = ttl.as(Int).seconds.from_now

      @read_servers = ConnectionPool.new(
        initial_pool_size: 0,
        max_pool_size: max_pool_size, # 0 == unlimited
        max_idle_pool_size: 10,
        checkout_timeout: 5.seconds,
        retry_attempts: 3,
        retry_delay: 200.milliseconds,
      ) do
        server = entrypoint.dup
        host, port = servers.as(Neo4j::List)
          .select { |value| value.as(Neo4j::Map)["role"] == "READ" }
          .flat_map { |value| value.as(Neo4j::Map)["addresses"].as(Array).map(&.as(String)) }
          .sample
          .split(':', 2)
        server.host = host
        server.port = port.to_i?

        Bolt::Connection.new(uri: server, ssl: ssl)
      end

      @write_servers = ConnectionPool.new(
        initial_pool_size: 1,
        max_pool_size: max_pool_size, # 0 == unlimited
        max_idle_pool_size: 10,
        checkout_timeout: 5.seconds,
        retry_attempts: 3,
        retry_delay: 200.milliseconds,
      ) do
        server = entrypoint.dup
        host, port = servers.as(Neo4j::List)
          .select { |value| value.as(Neo4j::Map)["role"] == "WRITE" }
          .flat_map { |value| value.as(Neo4j::Map)["addresses"].as(Array).map(&.as(String)) }
          .sample
          .split(':', 2)
        server.host = host
        server.port = port.to_i?

        Bolt::Connection.new(uri: server, ssl: ssl)
      end
    end

    def session(& : Session -> T) forall T
      session = Session.new(self)
      yield session
    ensure
      session.close if session
    end

    class Session < ::Neo4j::Session
      enum ConnectionType
        Pending
        Read
        Write
        Closed
      end

      delegate execute, stream, exec_cast, exec_cast_scalar, to: connection

      @connection : Bolt::Connection?
      @connection_type = ConnectionType::Pending

      def initialize(@cluster : Cluster)
      end

      def write_transaction
        raise SessionClosed.new("Cannot open a write transaction on a closed session") if closed?

        @connection_type = ConnectionType::Write

        connection.transaction { |txn| yield txn }
      end

      def read_transaction
        raise SessionClosed.new("Cannot open a write transaction on a closed session") if closed?

        @connection_type = ConnectionType::Read if @connection_type.pending?

        connection.transaction { |txn| yield txn }
      end

      def connection
        case @connection_type
        when .pending?
          @connection_type = ConnectionType::Write
          @connection = @cluster.@write_servers.checkout
        when .read?
          @connection ||= @cluster.@read_servers.checkout
        when .write?
          @connection ||= @cluster.@write_servers.checkout
        when .closed?
          raise SessionClosed.new("Cannot reopen a closed session")
        else
          raise "Invalid connection type: #{@connection_type.inspect}"
        end
      end

      def close : Nil
        if connection = @connection
          case @connection_type
          when .read?
            @cluster.@read_servers.release connection
          when .write?
            @cluster.@write_servers.release connection
          end
        end
      end

      def closed?
        @connection_type.closed?
      end
    end

    class Error < ::Exception
    end
    class NotAClusterURI < Error
    end
    class SessionClosed < Error
    end
  end
end
