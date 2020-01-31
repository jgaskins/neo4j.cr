require "uri"

require "./bolt/connection"
require "./connection_pool"
require "./session"

module Neo4j
  class Cluster
    @check_again_in : Time::Span
    @read_servers : ConnectionPool
    @write_servers : ConnectionPool
    @read_server_addresses = Array(String).new
    @write_server_addresses = Array(String).new

    def initialize(@entrypoint : URI, @ssl = true, @max_pool_size = 200)
      unless entrypoint.scheme == "bolt+routing" || entrypoint.scheme == "neo4j"
        raise NotAClusterURI.new("The cluster entrypoint should be a 'bolt+routing' or 'neo4j' URI. Got: #{entrypoint}")
      end

      @check_again_in, @read_server_addresses, @write_server_addresses, @read_servers, @write_servers = refresh_servers

      spawn do
        loop do
          sleep @check_again_in

          @check_again_in, @read_server_addresses, @write_server_addresses, @read_servers, @write_servers = refresh_servers(@read_servers, @write_servers)
        end
      end
    end

    def session(& : Session -> T) forall T
      session = Session.new(self)
      yield session
    end

    private def refresh_servers(read_servers : ConnectionPool? = nil, write_servers : ConnectionPool? = nil) : {Time::Span, Array(String), Array(String), ConnectionPool, ConnectionPool}
      entrypoint = @entrypoint.dup
      entrypoint.scheme = "bolt"

      connection = Bolt::Connection.new(entrypoint, ssl: @ssl)

      ttl, raw_servers = connection.execute("call dbms.cluster.routing.getServers()").data.first
      servers = raw_servers
        .as(Array)
        .map(&.as(Map))
      read_server_addresses = servers
        .select { |value| value["role"] == "READ" }
        .flat_map { |value| value["addresses"].as(Array).map(&.as(String)) }
        .sort
      write_server_addresses = servers
        .select { |value| value["role"] == "WRITE" }
        .flat_map { |value| value["addresses"].as(Array).map(&.as(String)) }
        .sort

      check_again_in = ttl.as(Int).seconds

      if read_server_addresses == @read_server_addresses && write_server_addresses == @write_server_addresses && read_servers && write_servers
        return {check_again_in, read_server_addresses, write_server_addresses, read_servers, write_servers}
      end

      read_servers = ConnectionPool.new(
        initial_pool_size: 0,
        max_pool_size: @max_pool_size, # 0 == unlimited
        max_idle_pool_size: 10,
        checkout_timeout: 5.seconds,
        retry_attempts: 3,
        retry_delay: 200.milliseconds,
      ) do
        host, port = read_server_addresses.sample.split(':', 2)

        server = entrypoint.dup
        server.host = host
        server.port = port.to_i?

        Bolt::Connection.new(uri: server, ssl: @ssl)
      end

      write_servers = ConnectionPool.new(
        initial_pool_size: 1,
        max_pool_size: @max_pool_size, # 0 == unlimited
        max_idle_pool_size: 10,
        checkout_timeout: 5.seconds,
        retry_attempts: 3,
        retry_delay: 200.milliseconds,
      ) do
        server = entrypoint.dup
        host, port = write_server_addresses
          .sample
          .split(':', 2)
        server.host = host
        server.port = port.to_i?

        Bolt::Connection.new(uri: server, ssl: @ssl)
      end

      {check_again_in, read_server_addresses, write_server_addresses, read_servers, write_servers}
    end

    class Session < ::Neo4j::Session
      def initialize(@cluster : Cluster)
      end

      def write_transaction
        @cluster.@write_servers.checkout(&.transaction { |txn| yield txn })
      end

      def read_transaction
        @cluster.@read_servers.checkout(&.transaction { |txn| yield txn })
      end

      def exec_cast(query : String, as types : Tuple(*T), **params) forall T
        @cluster.@write_servers.checkout do |connection|
          connection.exec_cast query, params, types
        end
      end

      def execute(query : String, **params)
        @cluster.@write_servers.checkout(&.execute(query, **params))
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
