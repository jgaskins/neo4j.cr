require "uuid"

require "./exceptions"
require "./pack_stream/unpacker"
require "./bolt/from_bolt"

module Neo4j
  annotation Field
  end
  annotation NodeLabels
  end
  annotation RelationshipType
  end
  module Mappable
  end

  module Serializable
    module Node
      macro included
        include ::Neo4j::Mappable

        getter node_id : Int64
        getter node_labels : Array(String)

        # Define a `new` and `from_rs` directly in the type, like JSON::Serializable
        # For proper overload resolution

        def self.new(unpacker : ::Neo4j::PackStream::Unpacker)
          instance = allocate
          instance.initialize(__unpacker_for_neo4j_serializable: unpacker)
          GC.add_finalizer(instance) if instance.responds_to?(:finalize)
          instance
        end

        def self.new(node : ::Neo4j::Node)
          io = IO::Memory.new
          # TODO: Benchmark against assigning node properties, I bet this is slow
          ::Neo4j::PackStream::Packer.new(io).write node
          from_bolt io.rewind
        end

        def self.from_bolt(unpacker : ::Neo4j::PackStream::Unpacker)
          new unpacker
        end

        def self.from_bolt(io : IO)
          new ::Neo4j::PackStream::Unpacker.new(io)
        end

        # Inject the class methods into subclasses as well

        macro inherited
          def self.new(unpacker : ::Neo4j::PackStream::Unpacker))
            super
          end

          def self.from_bolt(unpacker : ::Neo4j::PackStream::Unpacker)
            super
          end
        end
      end

      def initialize(*, __unpacker_for_neo4j_serializable unpacker : ::Neo4j::PackStream::Unpacker)
        {% begin %}
          {% properties = {} of Nil => Nil %}
          {% for ivar in @type.instance_vars %}
            {% ann = ivar.annotation(::Neo4j::Field) %}
            {% unless ann && ann[:ignore] || ivar.id == "node_id" || ivar.id == "node_labels" %}
              {%
                properties[ivar.id] = {
                  type:      ivar.type,
                  key:       ((ann && ann[:key]) || ivar).id.stringify,
                  default:   ivar.default_value,
                  nilable:   ivar.type.nilable?,
                  converter: ann && ann[:converter],
                }
              %}
            {% end %}
          {% end %}

          {% for name, value in properties %}
            %var{name} = nil
            %found{name} = false
          {% end %}

          unpacker.next_token # Structure
          unpacker.next_token # Node
          @node_id = unpacker.read_numeric.to_i64
          @node_labels = unpacker.read_array.map(&.as(String))

          token = unpacker.next_token # Property map byte marker
          property_count = token.size

          property_count.times do
            property_name = unpacker.read_string

            case property_name
              {% for name, value in properties %}
                when {{value[:key]}}
                  %found{name} = true
                  %var{name} =
                    {% if value[:converter] %}
                      {{value[:converter]}}.from_bolt(unpacker).as({{value[:type]}})
                    {% elsif value[:nilable] || value[:default] != nil %}
                      begin
                        %intermediate_value{name} = unpacker.read
                        # pp {{name.stringify}}: {{value}}, _value: %intermediate_value, _class: %intermediate_value.class
                        if %intermediate_value{name}
                          {% if type = value[:type].union_types.find { |t| t < Int } %}
                            %intermediate_value{name}.as(Int::Signed).to_i{{type.stringify.gsub(/\D+/, "").id}}
                          {% else %}
                            nil
                          {% end %}
                        end
                      end.as({{value[:type]}}?)
                    {% else %}
                      # Do not try this at home
                      {{value[:type]}}.from_bolt(unpacker.@lexer.@io).as({{value[:type]}})
                    {% end %}
              {% end %}
            else
              unpacker.read # Still need to consume the value
            end
          end

          {% for key, value in properties %}
            {% unless value[:nilable] || value[:default] != nil %}
              if %var{key}.is_a?(Nil) && !%found{key}
                raise ::Neo4j::PropertyMissing.new("Node with id #{@node_id} and labels #{@node_labels.inspect} is missing property {{(value[:key] || key).id}}")
              end
            {% end %}
          {% end %}

          {% for key, value in properties %}
            {% if value[:nilable] %}
              {% if value[:default] != nil %}
                @{{key}} = %found{key} ? %var{key} : {{value[:default]}}
              {% else %}
                @{{key}} = %var{key}
              {% end %}
            {% elsif value[:default] != nil %}
              @{{key}} = %var{key}.is_a?(Nil) ? {{value[:default]}} : %var{key}
            {% else %}
              @{{key}} = %var{key}.as({{value[:type]}})
            {% end %}
          {% end %}
        {% end %}
      end
    end

    module Relationship
      macro included
        include ::Neo4j::Mappable

        getter relationship_id : Int64
        getter relationship_type : String
        getter node_start : Int64
        getter node_end : Int64

        # Define a `new` and `from_rs` directly in the type, like JSON::Serializable
        # For proper overload resolution

        def self.new(unpacker : ::Neo4j::PackStream::Unpacker)
          instance = allocate
          instance.initialize(__unpacker_for_neo4j_serializable: unpacker)
          GC.add_finalizer(instance) if instance.responds_to?(:finalize)
          instance
        end

        def self.new(relationship : ::Neo4j::Relationship)
          io = IO::Memory.new
          # TODO: Benchmark against assigning relationship properties, I bet this is slow
          ::Neo4j::PackStream::Packer.new(io).write relationship
          from_bolt io.rewind
        end

        def self.from_bolt(unpacker : ::Neo4j::PackStream::Unpacker)
          new unpacker
        end

        def self.from_bolt(io : IO)
          new ::Neo4j::PackStream::Unpacker.new(io)
        end

        # Inject the class methods into subclasses as well

        macro inherited
          def self.new(unpacker : ::Neo4j::PackStream::Unpacker))
            super
          end

          def self.from_bolt(unpacker : ::Neo4j::PackStream::Unpacker)
            super
          end
        end
      end

      def initialize(*, __unpacker_for_neo4j_serializable unpacker : ::Neo4j::PackStream::Unpacker)
        {% begin %}
          {% properties = {} of Nil => Nil %}
          {% for ivar in @type.instance_vars %}
            {% ann = ivar.annotation(::Neo4j::Field) %}
            {% unless ann && ann[:ignore] || %w[relationship_id relationship_type node_start node_end].includes? ivar.stringify %}
              {%
                properties[ivar.id] = {
                  type:      ivar.type,
                  key:       ((ann && ann[:key]) || ivar).id.stringify,
                  default:   ivar.default_value,
                  nilable:   ivar.type.nilable?,
                  converter: ann && ann[:converter],
                }
              %}
            {% end %}
          {% end %}

          {% for name, value in properties %}
            %var{name} = nil
            %found{name} = false
          {% end %}

          unpacker.next_token # Structure
          unpacker.next_token # Relationship
          @relationship_id = unpacker.read_numeric.to_i64
          @node_start = unpacker.read_numeric.to_i64
          @node_end = unpacker.read_numeric.to_i64
          @relationship_type = unpacker.read_string

          token = unpacker.next_token # Property map byte marker
          property_count = token.size

          property_count.times do
            property_name = unpacker.read_string

            case property_name
              {% for name, value in properties %}
                when {{value[:key]}}
                  %found{name} = true
                  %var{name} =
                    {% if value[:converter] %}
                      {{value[:converter]}}.from_bolt(unpacker)
                    {% elsif value[:nilable] || value[:default] != nil %}
                      begin
                        %intermediate_value = unpacker.read
                        if %intermediate_value
                          {% if type = value[:type].union_types.find { |t| t < Int } %}
                            %intermediate_value.as(Int::Signed).to_i{{type.stringify.gsub(/\D+/, "").id}}
                          {% else %}
                            nil
                          {% end %}
                        end
                      end.as({{value[:type]}}?)
                    {% else %}
                      # Do not try this at home
                      {{value[:type]}}.from_bolt unpacker.@lexer.@io
                    {% end %}
              {% end %}
            else
              unpacker.read # Still need to consume the value
            end
          end

          {% for key, value in properties %}
            {% unless value[:nilable] || value[:default] != nil %}
              if %var{key}.nil? && !%found{key}
                raise ::Neo4j::PropertyMissing.new("Relationship with id #{@relationship_id} and type #{@relationship_type.inspect} is missing property {{(value[:key] || key).id}}")
              end
            {% end %}
          {% end %}

          {% for key, value in properties %}
            {% if value[:nilable] %}
              {% if value[:default] != nil %}
                @{{key}} = %found{key} ? %var{key} : {{value[:default]}}
              {% else %}
                @{{key}} = %var{key}
              {% end %}
            {% elsif value[:default] != nil %}
              @{{key}} = %var{key}.is_a?(Nil) ? {{value[:default]}} : %var{key}
            {% else %}
              @{{key}} = %var{key}.as({{value[:type]}})
            {% end %}
          {% end %}
        {% end %}
      end
    end
  end
end

struct UUID
  def self.from_bolt(unpacker : ::Neo4j::PackStream::Unpacker)
    UUID.new unpacker.read_string
  end

  def self.from_bolt(io : IO)
    UUID.new ::Neo4j::PackStream::Unpacker.new(io).read_string
  end
end
