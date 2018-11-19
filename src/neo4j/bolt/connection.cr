require "../type"
require "../pack_stream"
require "../pack_stream/packer"
require "../result"

require "socket"
require "openssl"

module Neo4j
  module Bolt
    class Connection
      GOGOBOLT = "\x60\x60\xB0\x17"
      SUPPORTED_VERSIONS = String.new(Bytes[
        0, 0, 0, 2,
        0, 0, 0, 0,
        0, 0, 0, 0,
        0, 0, 0, 0,
      ])
      COMMANDS = {
        init: 0x01,
        run: 0x10,
        pull_all: 0x3F,
        ack_failure: 0x0E,
        reset: 0x0F,
      }

      @connection : (TCPSocket | OpenSSL::SSL::Socket::Client)

      def initialize
        initialize "bolt://neo4j:neo4j@localhost:7687", ssl: false
      end

      def initialize(url : String)
        initialize URI.parse(url)
      end

      def initialize(url : String, ssl : Bool)
        initialize URI.parse(url), ssl
      end

      def initialize(@uri : URI, @ssl=true)
        host = uri.host.to_s
        port = uri.port || 7687
        username = uri.user.to_s
        password = uri.password.to_s

        if uri.scheme != "bolt"
          raise ArgumentError.new("Connection must use Bolt")
        end

        @connection = TCPSocket.new(host, port)

        if ssl
          context = OpenSSL::SSL::Context::Client.new
          context.add_options(OpenSSL::SSL::Options::NO_SSL_V2 | OpenSSL::SSL::Options::NO_SSL_V3)

          @connection = OpenSSL::SSL::Socket::Client.new(@connection, context)
        end

        @connection.write handshake
        @connection.flush
        server_version = @connection.read_bytes(
          Int32,
          IO::ByteFormat::BigEndian
        )

        init username, password
      end

      def execute(query, parameters : Hash(String, Type))
        Result.new(type: run(query, parameters), data: pull_all)
      end

      def execute(query, **params)
        params_hash = {} of String => Type

        params.each { |key, value| params_hash[key.to_s] = value }

        execute query, params_hash
      end

      def transaction
        execute "BEGIN"

        yield

        execute "COMMIT"
      rescue e : QueryException
        ack_failure
        run "ROLLBACK"
        reset
        raise e
      rescue e # Don't ack_failure if it wasn't a QueryException
        run "ROLLBACK"
        reset
        raise e
      end

      def close
        @connection.close
      end

      # If the connection gets into a wonky state, this method tells the server
      # to reset it back to a normal state, but you lose everything you haven't
      # pulled down yet.
      def reset
        write_message do |msg|
          msg.write_structure_start 0
          msg.write_byte COMMANDS[:reset]
        end
        read_result
      end

      private def init(username, password)
        write_message do |msg|
          msg.write_structure_start 1
          msg.write_byte COMMANDS[:init]
          msg.write "Neo4j.cr/0.1.0"
          msg.write({
            "scheme" => "basic",
            "principal" => username,
            "credentials" => password,
          })
        end

        case result = read_result
        when Success
          result
        when Failure
          raise Exception.new(result.inspect)
        else
          raise Exception.new("Don't know how to handle result: #{result.inspect}")
        end
      end

      private def write_message
        packer = PackStream::Packer.new
        yield packer

        slice = packer.to_slice
        length = slice.size

        message = IO::Memory.new.tap { |io|
          io.write_bytes length.to_u16, IO::ByteFormat::BigEndian
          io.write slice
          io.write_bytes 0x0000.to_u16, IO::ByteFormat::BigEndian
        }.to_slice
        @connection.write message
        @connection.flush
      end

      private def run(statement, parameters = {} of String => Type)
        write_message do |msg|
          msg.write_structure_start 2
          msg.write_byte COMMANDS[:run]
          msg.write statement
          msg.write parameters
        end

        result = read_result
        case result
        when Failure
          raise ::Neo4j::QueryException.new(result.attrs["message"].as(String), result.attrs["code"].as(String))
        when Success, Ignored
          result
        else
          raise ::Neo4j::UnknownResult.new("Cannot identify this result: #{result.inspect}")
        end
      rescue ex : IO::EOFError
        initialize @uri, @ssl
        run statement, parameters
      end

      private def pull_all
        write_message do |msg|
          msg.write_structure_start 0
          msg.write_byte COMMANDS[:pull_all]
        end

        results = Array(Array(Type)).new
        result = read_result
        if result.is_a? Failure
          raise ::Neo4j::QueryException.new(result.attrs["message"].as(String), result.attrs["code"].as(String))
        end

        until result.is_a?(Success) || result.is_a?(Ignored)
          results << result.as(Array(Type))
          result = read_result

          if result.is_a? Failure
            raise ::Neo4j::QueryException.new(result.attrs["message"].as(String), result.attrs["code"].as(String))
          end
        end

        results
      end

      # Read a single result from the server in 64kb chunks. This is a bit of a
      # naive implementation that buffers the chunks until it has the full
      # result, then flattens out the chunks into a single slice and parses
      # that slice. 
      private def read_result
        length = 1_u16
        messages = [] of Bytes
        length = @connection.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        while length != 0x0000
          bytes = Bytes.new(length)
          @connection.read_fully bytes
          messages << bytes
          length = @connection.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        end

        bytes = Bytes.new(messages.map(&.size).sum)
        current_byte = 0
        messages.reduce(bytes) do |chunk, msg|
          msg.copy_to chunk
          current_byte += msg.size

          bytes + current_byte
        end

        PackStream.unpack(bytes)
      end

      private def write(value)
        @connection.write_bytes value, IO::ByteFormat::BigEndian
        @connection.flush
      end

      private def write_value(value)
        @connection.write PackStream.pack(value)
      end

      private def handshake
        (GOGOBOLT + SUPPORTED_VERSIONS).to_slice
      end

      private def ack_failure
        write_message do |msg|
          msg.write_structure_start 0
          msg.write_byte COMMANDS[:ack_failure]
        end
        read_result
      end

      private def send_message(string : String)
        send_message string.to_slice
      end

      private def send_message(bytes : Bytes)
        @connection.write bytes
      end
    end
  end
end
