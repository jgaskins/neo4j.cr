require "uri"
require "socket"
require "openssl"

require "./neo4j/pack_stream"
require "./neo4j/mapping"

module Neo4j
  module Bolt
    class Connection
      GOGOBOLT = "\x60\x60\xB0\x17"
      SUPPORTED_VERSIONS = String.new(Bytes[
        0, 0, 0, 1,
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

      @host = "localhost"
      @port = 7687
      @username = "neo4j"
      @password = "neo4j"

      @connection : (TCPSocket | OpenSSL::SSL::Socket::Client)

      def initialize
        initialize "bolt://neo4j:neo4j@localhost:7687"
      end

      def initialize(url : String)
        initialize URI.parse(url)
      end

      def initialize(url : String, ssl : Bool)
        initialize URI.parse(url), ssl
      end

      def initialize(uri : URI, ssl=true)
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

      def init(username, password)
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
        read_response
      end

      def write_message
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

      def run(statement, parameters = {} of String => Type)
        write_message do |msg|
          msg.write_structure_start 2
          msg.write_byte COMMANDS[:run]
          msg.write statement
          msg.write parameters
        end

        result = read_response
        case result
        when Failure
          raise ::Neo4j::QueryException.new(result.attrs["message"].as(String), result.attrs["code"].as(String))
        when Success, Ignored
          result
        else
          raise ::Neo4j::UnknownResult.new("Cannot identify this result: #{result.inspect}")
        end
      end

      def pull_all
        write_message do |msg|
          msg.write_structure_start 0
          msg.write_byte COMMANDS[:pull_all]
        end

        results = Array(Array(Type)).new
        result = read_response
        if result.is_a? Failure
          raise ::Neo4j::QueryException.new(result.attrs["message"].as(String), result.attrs["code"].as(String))
        end

        until result.is_a?(Success) || result.is_a?(Ignored)
          results << result.as(Array(Type))
          result = read_response

          if result.is_a? Failure
            raise ::Neo4j::QueryException.new(result.attrs["message"].as(String), result.attrs["code"].as(String))
          end
        end

        results
      end

      def read_response
        length = @connection.read_bytes(UInt16, IO::ByteFormat::BigEndian)
        bytes = Bytes.new(length)
        @connection.read_fully bytes
        footer = @connection.read_bytes UInt16
        if footer != 0
          raise "Footer is wrong: #{footer.to_s(16)}"
        end

        PackStream.unpack(bytes)
      end

      def write(value)
        @connection.write_bytes value, IO::ByteFormat::BigEndian
        @connection.flush
      end

      def write_footer
        write 0_u16
      end

      def write_value(value)
        @connection.write PackStream.pack(value)
      end

      def handshake
        (GOGOBOLT + SUPPORTED_VERSIONS).to_slice
      end
      
      def transaction
        execute "BEGIN"

        yield

        execute "COMMIT"
      rescue e
        ack_failure
        run "ROLLBACK"
        reset
        raise e
      end

      def ack_failure
        write_message do |msg|
          msg.write_structure_start 0
          msg.write_byte COMMANDS[:ack_failure]
        end
        read_response
      end

      def reset
        write_message do |msg|
          msg.write_structure_start 0
          msg.write_byte COMMANDS[:reset]
        end
        read_response
      end

      def execute(query, parameters = {} of String => Type)
        # benchmark "Cypher query" do
          Result.new(
            type: run(query, parameters),
            data: pull_all,
          )
        # end
      end

      def close
        @connection.close
      end

      private def benchmark(message)
        start = Time.now
        result = yield
        finish = Time.now
        puts "#{message}: #{finish - start}"

        result
      end

      private def send_message(string : String)
        send_message string.to_slice
      end

      private def send_message(bytes : Bytes)
        @connection.write bytes
      end
    end

    struct Node
      getter(
        id : Int64,
        labels : Array(String),
        properties : Hash(String, Type),
      )

      def initialize(@id, @labels, @properties)
      end
    end
  end

  class Exception < ::Exception
  end

  class QueryException < Exception
    getter code : String

    def initialize(message, @code)
      super "#{message} [#{code}]"
    end
  end

  class UnknownResult < Exception
    def initialize(@message)
    end
  end

  class Result
    include Enumerable(Array(Type))

    getter type : Success | Ignored
    getter data : Array(Array(Type))

    def initialize(@type, @data)
    end

    def each
      @data.each do |row|
        yield row
      end
    end

    def size
      @data.size
    end

    def fields
      @type.fields
    end
  end
end
