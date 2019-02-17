module Neo4j
  module PackStream
    struct Packer
      def initialize(@io : IO = IO::Memory.new)
      end

      def write(value : Nil)
        write_byte(0xC0)
        self
      end

      def write(value : Bool)
        write_byte(value ? 0xC3 : 0xC2)
        self
      end

      def write_string_start(bytesize)
        case bytesize
        when (0x0..0xF)
          # fixraw
          write_byte(0x80 + bytesize)
        when (0x00..0xFF)
          # str8
          write_byte(0xD0)
          write_value(bytesize.to_u8)
        when (0x0000..0xFFFF)
          # str16
          write_byte(0xD1)
          write_value(bytesize.to_u16)
        when (0x0000_0000..0xFFFF_FFFF)
          # str32
          write_byte(0xD2)
          write_value(bytesize.to_u32)
        else
          raise Error.new("invalid length")
        end
        self
      end

      def write(value : String)
        write_string_start(value.bytesize)
        write_slice(value.to_slice)
        self
      end

      def write(value : Symbol)
        write(value.to_s)
      end

      def write(value : Float32 | Float64)
        write_byte(0xC1)
        write_value(value.to_f64)
        self
      end

      def write(value : Int8 | Int16 | Int32 | Int64)
        if value >= 0
          if Int8::MAX >= value
            write_byte(0xC8)
            write_byte(value.to_u8)
          elsif Int16::MAX >= value
            write_byte(0xC9)
            write_value(value.to_u16)
          elsif Int32::MAX >= value
            write_byte(0xCA)
            write_value(value.to_u32)
          else
            write_byte(0xCB)
            write_value(value.to_u64)
          end
        else
          if Int8::MIN <= value
            write_byte(0xC8)
            write_byte(value.to_i8)
          elsif Int16::MIN <= value
            write_byte(0xC9)
            write_value(value.to_i16)
          elsif Int32::MIN <= value
            write_byte(0xCA)
            write_value(value.to_i32)
          else
            write_byte(0xCB)
            write_value(value.to_i64)
          end
        end
        self
      end

      def write(value : Hash)
        write_hash_start(value.size)

        value.each do |key, value|
          write(key)
          write(value)
        end

        self
      end

      def write_hash_start(length)
        case length
        when (0x0..0xF)
          write_byte(0xA0 + length)
        when (0x00..0xFF)
          write_byte(0xD8)
          write_value(length.to_u8)
        when (0x0000..0xFFFF)
          write_byte(0xD9)
          write_value(length.to_u16)
        when (0x0000_0000..0xFFFF_FFFF)
          write_byte(0xDA)
          write_value(length.to_u32)
        else
          raise Error.new("invalid length")
        end
        self
      end

      def write(value : Array)
        write_array_start(value.size)
        value.each { |item| write(item) }
        self
      end

      def write_array_start(length)
        case length
        when (0x00..0x0F)
          write_byte(0x90 + length)
        when (0x00..0xFF)
          write_byte(0xD4)
          write_value(length.to_u8)
        when (0x0000..0xFFFF)
          write_byte(0xD5)
          write_value(length.to_u16)
        when (0x0000_0000..0xFFFF_FFFF)
          write_byte(0xD6)
          write_value(length.to_u32)
        else
          raise Error.new("invalid length")
        end
        self
      end

      def write_structure_start(length)
        case length
        when (0x0..0xF)
          write_byte(0xB0 + length)
        when (0x00..0xFF)
          write_byte(0xDD)
          write_value(length.to_u8)
        when (0x0000..0xFFFF)
          write_byte(0xDC)
          write_value(length.to_u16)
        else
          raise Error.new("invalid length")
        end
        self
      end

      def write(time : Time)
        write_structure_start 3
        write_byte Unpacker::StructureTypes::DateTime.value
        write time.to_unix.to_i64 + time.offset.to_i32
        write time.nanosecond.to_i32
        write time.offset.to_i32
        self
      end

      def write(point : Point2D)
        write_structure_start 3
        write_byte Unpacker::StructureTypes::Point2D.value
        write 7203_i16
        write point.x
        write point.y
        self
      end

      def write(point : Point3D)
        write_structure_start 4
        write_byte Unpacker::StructureTypes::Point3D.value
        write 9157_i16
        write point.x
        write point.y
        write point.z
        self
      end

      def write(latlng : LatLng)
        write_structure_start 3
        write_byte Unpacker::StructureTypes::Point2D.value
        write 4326_i16
        write latlng.longitude
        write latlng.latitude
        self
      end

      def write(node : Node)
        write_structure_start 3
        write_byte Unpacker::StructureTypes::Node.value
        write node.id
        write node.labels
        write node.properties
        self
      end

      def write(rel : Relationship)
        write_structure_start 5
        write_byte 0x52
        write rel.id
        write rel.start
        write rel.end
        write rel.type
        write rel.properties
        self
      end

      def write(path : Path)
        write_byte 0x50
        write path.nodes
        write path.relationships
        write path.sequence
        self
      end

      def write(rel : UnboundRelationship)
        write_byte 0x72
        write rel.id
        write rel.type
        write rel.properties
        self
      end

      def write(result : Success)
        write_byte 0x70
        write result.attrs
        self
      end

      def write(result : Failure)
        write_byte 0x7F
        write result.attrs
        self
      end

      def write(result : Ignored)
        write_byte 0x7E
        self
      end

      def write(value : Tuple)
        write_array_start(value.size)
        value.each { |item| write(item) }
        self
      end

      def write_byte(byte)
        @io.write_byte(byte.to_u8)
      end

      private def write_value(value)
        @io.write_bytes(value, IO::ByteFormat::BigEndian)
      end

      private def write_slice(slice)
        @io.write(slice)
      end

      def to_slice
        io = @io
        if io.responds_to?(:to_slice)
          io.to_slice
        else
          raise "to slice not implemented for io type: #{typeof(io)}"
        end
      end

      def to_s
        @io.to_s
      end

      def bytes
        @io.to_s.bytes
      end
    end
  end
end
