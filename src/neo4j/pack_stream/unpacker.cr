require "./lexer"

module Neo4j
  module PackStream
    struct Unpacker
      enum StructureTypes : Int8
        # Primitive Types
        Node                = 0x4e
        Relationship        = 0x52
        Path                = 0x50
        UnboundRelationship = 0x72
        Record              = 0x71

        # Result Types
        Success             = 0x70
        Failure             = 0x7f
        Ignored             = 0x7e

        # Temporal Types
        DateTimeWithOffset  = 0x46
        DateTimeWithTZ      = 0x66
        LocalDateTime       = 0x64
        Date                = 0x44
        LocalTime           = 0x74
        Time                = 0x54
        Duration            = 0x45

        # Spatial Types
        Point2D             = 0x58
        Point3D             = 0x59
      end

      def initialize(string_or_io)
        @lexer = Lexer.new(string_or_io)
      end

      def read
        read_value
      end

      def read_nil
        next_token
        check Token::Type::Null
        nil
      end

      def read_bool
        next_token
        case token.type
        when .true?
          true
        when .false?
          false
        else
          unexpected_token
        end
      end

      def read_numeric
        next_token
        case token.type
        when .int?
          token.int_value
        when .float?
          token.float_value
        else
          unexpected_token
        end
      end

      {% for type in %w(Int Float String) %}
        def read_{{type.id.downcase}}      # def read_int
          next_token                       #   next_token
          check Token::Type::{{type.id}}   #   check Token::Type::Int
          token.{{type.id.downcase}}_value #   token.int_value
        end                                # end
      {% end %}

      def read_array(fetch_next_token = true) : List
        next_token if fetch_next_token
        check Token::Type::Array
        List.new(token.size.to_i32) do
          read_value.as(Value)
        end
      end

      def read_hash(fetch_next_token = true) : Map
        next_token if fetch_next_token
        check Token::Type::Hash
        hash = Map.new(initial_capacity: token.size.to_i32)
        token.size.times do
          hash[read_string] = read_value.as(Value)
        end
        hash
      end

      def read_structure(fetch_next_token = true)
        next_token if fetch_next_token
        check Token::Type::Structure

        structure_type = read_value

        case structure_type
        when StructureTypes::Node.value
          Node.new(
            id: read_numeric.to_i64,
            labels: read_array.map(&.as(String)),
            properties: read_hash.transform_keys(&.to_s),
          )
        when StructureTypes::Relationship.value
          Relationship.new(
            id: read_numeric.to_i64,
            start: read_numeric.to_i64,
            end: read_numeric.to_i64,
            type: read_string,
            properties: read_hash.transform_keys(&.to_s),
          )
        when StructureTypes::Path.value
          Path.new(
            nodes: read_array.map(&.as(Node)),
            relationships: read_array.map(&.as(UnboundRelationship)),
            sequence: read_array.map(&.as(Int8)),
          )
        when StructureTypes::UnboundRelationship.value
          UnboundRelationship.new(
            id: read_numeric.to_i64,
            type: read_string,
            properties: read_hash.transform_keys(&.to_s),
          )
        when StructureTypes::Success.value
          Success.new(read_hash)
        when StructureTypes::Failure.value
          Failure.new(read_hash)
        when StructureTypes::Ignored.value
          Ignored.new
        when StructureTypes::Record.value
          read_value

        # Date/time types
        when StructureTypes::DateTimeWithOffset.value
          time = Time.unix(read_int) + read_int.nanoseconds
          offset = read_int.to_i32
          (time - offset.seconds).in(Time::Location.fixed(offset))
        when StructureTypes::LocalDateTime.value
          Time.unix(read_int) + read_int.nanoseconds
        when StructureTypes::Date.value
          Time::UNIX_EPOCH + read_int.days
        when StructureTypes::LocalTime.value
          Time::UNIX_EPOCH + read_int.nanoseconds
        when StructureTypes::Time.value
          time = Time::UNIX_EPOCH + read_int.nanoseconds
          offset = read_int.to_i32
          (time - offset.seconds).in(Time::Location.fixed(offset))
        when StructureTypes::Point2D.value
          type = read_int.to_i16
          case type
          when 7203
            Point2D.new(type: type, x: read_float, y: read_float)
          when 4326
            LatLng.new(type: type, longitude: read_float, latitude: read_float)
          end
        when StructureTypes::Point3D.value
          Point3D.new(
            type: read_int.to_i16,
            x: read_float,
            y: read_float,
            z: read_float,
          )
        when StructureTypes::Duration.value
          Duration.new(
            months: read_int,
            days: read_int,
            seconds: read_int,
            nanoseconds: read_int,
          )

        when StructureTypes::DateTimeWithTZ.value
          seconds = read_int.to_i64
          nanoseconds = read_int.to_i32
          location = Time::Location.load(read_string)
          Time.new(year: 1970, month: 1, day: 1, location: location) + seconds.seconds + nanoseconds.nanoseconds
        else
          Array(Value).new(token.size) do
            read_value.as Value
          end
        end
      end

      def read_structure(fetch_next_token = true)
        next_token if fetch_next_token

        check Token::Type::Structure
        token.size.times { yield }
      end

      def read_value
        next_token

        case token.type
        when .int?
          token.int_value
        when .float?
          token.float_value
        when .string?
          token.string_value
        when .null?
          nil
        when .true?
          true
        when .false?
          false
        when .array?
          read_array fetch_next_token: false
        when .hash?
          read_hash fetch_next_token: false
        when .structure?
          read_structure fetch_next_token: false
        else
          unexpected_token token.type
        end
      end

      delegate token, to: @lexer
      delegate next_token, to: @lexer
      delegate prefetch_token, to: @lexer

      def check(token_type)
        unexpected_token(token_type) unless token.type == token_type
      end

      private def unexpected_token(token_type = nil)
        message = "unexpected token '#{token}'"
        message += " expected #{token_type}" if token_type
        raise UnpackException.new(message, @lexer.byte_number)
      end
    end
  end
end
