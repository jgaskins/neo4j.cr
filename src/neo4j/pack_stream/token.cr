module Neo4j
  module PackStream
    class Token
      property type

      property string_value
      property int_value : Int8 | Int16 | Int32 | Int64
      property float_value : Float64
      property size : UInt16
      property used

      def initialize
        @type = :EOF
        @string_value = ""
        @int_value = 0_i8
        @float_value = 0.0_f64

        @size = 0_u16
        @used = true
      end

      def size=(size)
        @size = size.to_u16
      end

      def to_s(io)
        case @type
        when :nil
          io << :nil
        when :STRING
          @string_value.inspect(io)
        when :INT
          io << @int_value
        when :FLOAT
          io << @float_value
        else
          io << @type
        end
      end
    end
  end
end
