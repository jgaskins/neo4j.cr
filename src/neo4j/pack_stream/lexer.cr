require "./token"
require "../pack_stream"

module Neo4j
  module PackStream
    struct Lexer
      getter token
      getter current_byte
      getter byte_number

      def initialize(string : String)
        initialize IO::Memory.new(string)
      end

      def initialize(slice : Bytes)
        initialize IO::Memory.new(slice)
      end

      def initialize(@io : IO)
        @token = Token.new
        @byte_number = 0_u64
        @current_byte = 0_u8
        @eof = false
      end

      def prefetch_token
        return @token unless @token.used
        next_byte

        return @token if @eof

        @token.used = false

        # p current_byte: "0x%02x" % current_byte

        case current_byte
        when 0xC0
          set_type_and_size(Token::Type::Null, 0)
        when 0xC2
          set_type_and_size(Token::Type::False, 0)
        when 0xC3
          set_type_and_size(Token::Type::True, 0)
        when 0x80..0x8F
          consume_string(current_byte - 0x80)
        when 0xD0
          consume_string(read Int8)
        when 0xD1
          consume_string(read Int16)
        when 0xD2
          consume_string(read Int32)
        when 0xC1
          consume_float(read Float64)
        when 0xC8
          consume_int(read Int8)
        when 0xC9
          consume_int(read Int16)
        when 0xCA
          consume_int(read Int32)
        when 0xCB
          consume_int(read Int64)
        when 0x90..0x9F
          set_type_and_size(Token::Type::Array, current_byte - 0x90)
        when 0xD4
          set_type_and_size(Token::Type::Array, read UInt8)
        when 0xD5
          set_type_and_size(Token::Type::Array, read UInt16)
        when 0xD6
          set_type_and_size(Token::Type::Array, read UInt32)
        when 0xA0..0xAF
          set_type_and_size(Token::Type::Hash, current_byte - 0xA0)
        when 0xD8
          set_type_and_size(Token::Type::Hash, read UInt8)
        when 0xD9
          set_type_and_size(Token::Type::Hash, read UInt16)
        when 0xDA
          set_type_and_size(Token::Type::Hash, read UInt32)
        when 0xB0..0xBF
          set_type_and_size(Token::Type::Structure, current_byte - 0xB0)
        when 0xDC
          set_type_and_size(Token::Type::Structure, read UInt8)
        when 0xDD
          set_type_and_size(Token::Type::Structure, read UInt16)

        # If we've gotten this far, I think it means it's a TINY_INT
        when 0x00..0x7F
          @token.type = Token::Type::Int
          @token.int_value = current_byte.to_i8
        when 0xF0..0xFF
          @token.type = Token::Type::Int
          # Allow overflow to make this negative, because that's actually the
          # point of this range of values.
          @token.int_value = current_byte.to_i8!
        else
          unexpected_byte!
        end

        @token
      end

      def next_token
        token = prefetch_token
        token.used = true
        token
      end

      private def next_byte
        @byte_number += 1
        byte = @io.read_byte

        unless byte
          @eof = true
          @token.type = Token::Type::Eof
        end

        @current_byte = byte || 0.to_u8
      end

      private def set_type_and_size(type, size)
        # p type: type, size: size
        @token.type = type
        @token.size = size
      end

      private def consume_int(value)
        @token.type = Token::Type::Int
        @token.int_value = value
      end

      private def consume_float(value)
        @token.type = Token::Type::Float
        @token.float_value = value
      end

      private def consume_string(size)
        size = size.to_u32
        @token.type = Token::Type::String
        @token.string_value = String.new(size) do |buffer|
          @io.read_fully(Slice.new(buffer, size))
          {size, 0}
        end
        @byte_number += size
      end

      private def read(type : T.class) forall T
        @byte_number += sizeof(T)
        @io.read_bytes(T, IO::ByteFormat::BigEndian)
      end

      private def unexpected_byte!(byte = current_byte)
        raise UnpackException.new("Unexpected byte '#{byte}'", @byte_number)
      end
    end
  end
end
