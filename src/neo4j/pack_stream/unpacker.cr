require "./lexer"

module Neo4j
  module PackStream
    struct Unpacker
      STRUCTURE_TYPES = {
        node: 0x4e,
        relationship: 0x52,
        path: 0x50,
        unbound_relationship: 0x72,
        success: 0x70,
        failure: 0x7f,
        ignored: 0x7e,
        record: 0x71,

        # All the date/time-related types
        datetime: 0x46,
        localdatetime: 0x64,
        date: 0x44,
        localtime: 0x74,
        time: 0x54,
        duration: 0x45,

        # Points
        point2d: 0x58,
        point3d: 0x59,
      }

      def initialize(string_or_io)
        @lexer = Lexer.new(string_or_io)
      end

      def read
        read_value
      end

      def read_nil
        next_token
        check :nil
        nil
      end

      def read_bool
        next_token
        case token.type
        when :true
          true
        when :false
          false
        else
          unexpected_token
        end
      end

      def read_numeric
        next_token
        case token.type
        when :INT
          token.int_value
        when :FLOAT
          token.float_value
        else
          unexpected_token
        end
      end

      {% for type in %w(int uint float string binary) %}
        def read_{{type.id}}                          # def read_int
          next_token
          check :{{type.id.upcase}}                   #   check :INT
          token.{{type.id}}_value                     #   token.int_value
        end                                           # end
      {% end %}

      def read_array(fetch_next_token = true)
        next_token if fetch_next_token
        check :ARRAY
        Array(Type).new(token.size.to_i32) do
          read_value
        end
      end

      def read_hash(fetch_next_token = true)
        next_token if fetch_next_token
        check :HASH
        hash = Hash(String, Type).new(initial_capacity: token.size.to_i32)
        token.size.times do
          key = read_string
          value = read_value
          hash[key] = value
        end
        hash
      end

      def read_structure(fetch_next_token = true)
        next_token if fetch_next_token
        check :STRUCTURE

        structure_type = read_value

        case structure_type
        when STRUCTURE_TYPES[:node]
          id = read_numeric.to_i32
          labels = read_array.map(&.to_s)
          props = read_hash
            .each_with_object({} of String => Type) { |(k, v), h|
              h[k.to_s] = v }
          Node.new(id, labels, props)
        when STRUCTURE_TYPES[:relationship]
          Relationship.new(
            id: read_numeric.to_i32,
            start: read_numeric.to_i32,
            end: read_numeric.to_i32,
            type: read_string,
            properties: read_hash
              .each_with_object({} of String => Type) { |(k, v), h|
                h[k.to_s] = v }
          )
        when STRUCTURE_TYPES[:path]
          Path.new(
            nodes: read_array.map { |node| node.as(Node) },
            relationships: read_array.map(&.as(UnboundRelationship)),
            sequence: read_array.map(&.as(Int8)),
          )
        when STRUCTURE_TYPES[:unbound_relationship]
          UnboundRelationship.new(
            id: read_numeric.to_i32,
            type: read_string,
            properties: read_hash
              .each_with_object({} of String => Type) { |(k, v), h|
                h[k.to_s] = v }
          )
        when STRUCTURE_TYPES[:success]
          Success.new(read_hash)
        when STRUCTURE_TYPES[:failure]
          Failure.new(read_hash)
        when STRUCTURE_TYPES[:ignored]
          Ignored.new
        when STRUCTURE_TYPES[:record]
          read_value

        # Date/time types
        when STRUCTURE_TYPES[:datetime]
          time = Time.unix(read_int) + read_int.nanoseconds
          offset = read_int.to_i32
          (time - offset.seconds).in(Time::Location.fixed(offset))
        when STRUCTURE_TYPES[:localdatetime]
          Time.unix(read_int) + read_int.nanoseconds
        when STRUCTURE_TYPES[:date]
          Time::UNIX_EPOCH + read_int.days
        when STRUCTURE_TYPES[:localtime]
          Time::UNIX_EPOCH + read_int.nanoseconds
        when STRUCTURE_TYPES[:time]
          time = Time::UNIX_EPOCH + read_int.nanoseconds
          offset = read_int.to_i32
          (time - offset.seconds).in(Time::Location.fixed(offset))
        when STRUCTURE_TYPES[:point2d]
          type = read_int.to_i16
          case type
          when 7203
            Point2D.new(type: type, x: read_float, y: read_float)
          when 4326
            LatLng.new(type: type, longitude: read_float, latitude: read_float)
          end
        when STRUCTURE_TYPES[:point3d]
          Point3D.new(
            type: read_int.to_i16,
            x: read_float,
            y: read_float,
            z: read_float,
          )

        # TODO: Figure out how to represent Time::Span and Time::MonthSpan in the same object
        # when STRUCTURE_TYPES[:duration]
        #   Time::Span.new(months: read_int, days: read_int, seconds: read_int, nanoseconds: read_int)
        else
          Array(Type).new(token.size) do
            read_value
          end
        end
      end

      def read_structure(fetch_next_token = true)
        next_token if fetch_next_token

        check :STRUCTURE
        token.size.times { yield }
      end

      def read_value : Type
        next_token

        case token.type
        when :INT
          token.int_value
        when :FLOAT
          token.float_value
        when :STRING
          token.string_value
        when :nil
          nil
        when :true
          true
        when :false
          false
        when :ARRAY
          read_array fetch_next_token: false
        when :HASH
          read_hash fetch_next_token: false
        when :STRUCTURE
          read_structure fetch_next_token: false
        else
          unexpected_token token.type
        end
      end

      private delegate token, to: @lexer
      private delegate next_token, to: @lexer
      delegate prefetch_token, to: @lexer

      private def check(token_type)
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
