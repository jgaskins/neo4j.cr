require "./type"
require "./pack_stream/packer"
require "./pack_stream/unpacker"

module Neo4j
  module PackStream
    def self.pack(value : Type)
      Packer.new.write(value).to_slice
    end

    def self.unpack(string : String)
      unpack IO::Memory.new(string)
    end

    def self.unpack(io)
      Unpacker.new(io).read
    end

    class Error < Exception
    end

    class UnpackException < Error
      getter byte_number : UInt64

      def initialize(message, @byte_number)
        super "#{message} at #{@byte_number}"
      end
    end
  end
end
