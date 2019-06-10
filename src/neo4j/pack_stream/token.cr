module Neo4j
  module PackStream
    class Token
      enum Type
        Eof
        Null
        False
        True
        Array
        Hash
        Structure
        Int
        Float
        String
      end

      property type

      property string_value
      property int_value : Int8 | Int16 | Int32 | Int64
      property float_value : Float64
      property size : UInt64
      property used

      def initialize
        @type = Type::Eof
        @string_value = ""
        @int_value = 0_i8
        @float_value = 0.0_f64

        @size = 0_u16
        @used = true
      end

      def size=(size)
        @size = size.to_u64
      end

      def to_s(io)
        case @type
        when .string?
          @string_value.inspect(io)
        when .int?
          io << @int_value
        when .float?
          io << @float_value
        else
          io << ":#{@type.to_s.upcase}"
        end
      end
    end
  end
end
