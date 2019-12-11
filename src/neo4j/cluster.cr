require "uri"

require "./bolt/connection"
require "./connection_pool"

module Neo4j
  class Cluster
    @check_again_at : Time
    @write_servers : ConnectionPool
    @read_servers : ConnectionPool

    def initialize(entrypoint : URI, ssl = true)
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
        max_pool_size: 0, # 0 == unlimited
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
        max_pool_size: 0, # 0 == unlimited
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

    def write_transaction
      connection = @write_servers.checkout
      connection.transaction do
        yield connection
      end
    ensure
      @write_servers.release connection if connection
    end

    def read_transaction
      connection = @read_servers.checkout
      connection.transaction do
        yield connection
      end
    ensure
      @read_servers.release connection if connection
    end

    class Error < ::Exception
    end
    class NotAClusterURI < Error
    end
  end

  class Session
    def initialize(@connection : Bolt::Connection)
    end

    def run
    end
  end
end
