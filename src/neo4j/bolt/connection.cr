require "../version"
require "../type"
require "../pack_stream"
require "../pack_stream/packer"
require "../result"
require "./transaction"
require "./from_bolt"

require "socket"
require "openssl"

module Neo4j
  module Bolt
    class Connection
      GOGOBOLT           = "\x60\x60\xB0\x17".to_slice
      SUPPORTED_VERSIONS = {4, 0, 0, 0}
      enum Commands
        Hello    = 0x01
        Goodbye  = 0x02
        Reset    = 0x0f
        Run      = 0x10
        Begin    = 0x11
        Commit   = 0x12
        Rollback = 0x13
        Discard  = 0x2f
        Pull     = 0x3f
      end

      @connection : (TCPSocket | OpenSSL::SSL::Socket::Client)
      @transaction : Transaction?
      @data_waiting = false

      def initialize
        initialize "bolt://neo4j:neo4j@localhost:7687", ssl: false
      end

      # Initializes this connection with the given URL string and SSL flag.
      #
      # ```
      # connection = Neo4j::Bolt::Connection.new("bolt://neo4j:password@localhost", ssl: false)
      # ```
      def initialize(url : String, ssl : Bool = true)
        initialize URI.parse(url), ssl
      end

      # Initializes this connection with the given URI and SSL flag. SSL
      # defaults to `true` so that, if you omit it by mistake, you aren't
      # sending your database credentials in plaintext.
      #
      # ```
      # uri = URI.parse("bolt://neo4j:password@localhost")
      # connection = Neo4j::Bolt::Connection.new(uri, ssl: false)
      # ```
      def initialize(@uri : URI, @ssl = true, connection_retries = 5)
        host = uri.host.to_s
        port = uri.port || 7687
        username = uri.user.to_s
        password = uri.password.to_s

        if uri.scheme != "bolt"
          raise ::ArgumentError.new("Connection must use Bolt")
        end

        @connection = loop do
          break TCPSocket.new(host, port)
        rescue ex
          connection_retries -= 1
          if connection_retries < 0
            raise ex
          end
        end

        if ssl
          context = OpenSSL::SSL::Context::Client.new
          context.add_options(OpenSSL::SSL::Options::NO_SSL_V2 | OpenSSL::SSL::Options::NO_SSL_V3)

          # Must set hostname for SNI (Server Name Indication) on some platforms, such as Neo4j Aura
          @connection = OpenSSL::SSL::Socket::Client.new(@connection, context, hostname: uri.host)
        end

        handshake

        init username, password
      end

      def execute(_query, **parameters, &block : List ->)
        params_hash = Map.new

        parameters.each do |key, value|
          params_hash[key.to_s] = value.to_bolt_params
        end

        execute _query, params_hash, &block
      end

      # Executes the given query with the given parameters and executes the
      # block once for each result returned from the database.
      #
      # ```
      # connection.execute <<-CYPHER, Neo4j::Map{"id" => 123} do |(user)|
      #   MATCH (user:User { id: $id })
      #   RETURN user
      # CYPHER
      #   process User.new(user.as Neo4j::Node)
      # end
      # ```
      #
      # In the example above, we used `()` in the block arguments to destructure
      # the list of `RETURN`ed values. Also note that we need to cast it down to
      # a `Neo4j::Node`. All values in results have the compile-time type of
      # `Neo4j::Value` and so will need to be cast down to its specific type.
      def execute(query, parameters : Map, metadata = Map.new, &block : List ->)
        send Commands::Run, query, parameters, metadata
        send Commands::Pull, Map{"n" => -1}

        response = read_result.as(Response) # RUN
        result = read_result
        until result.is_a?(Neo4j::Response)
          yield result.as(List)
          result = read_result
        end

        handle_result response
        {response, result.as(Response)}
      end

      # Execute the given query with the given parameters, returning a Result
      # object containing query metadata and the query results in an array.
      #
      # ```
      # connection.execute(query, Neo4j::Map{"id" => 123})
      # ```
      def execute(query, parameters : Map, metadata = Map.new)
        Result.new(type: run(query, parameters, metadata), data: pull_all)
      rescue e
        begin
          reset unless @transaction # Let the transaction handle this
        rescue another_ex
        end
        raise e
      end

      # Execute the given query with the given parameters, returning a Result
      # object containing query metadata and the query results in an array.
      #
      # This method provides a convenient shorthand for providing a `Neo4j::Map`
      # of query parameters.
      #
      # ```
      # connection.execute(query, id: 123)
      # ```
      def execute(_query query, **params)
        params_hash = Map.new

        params.each { |key, value| params_hash[key.to_s] = value.to_bolt_params }

        execute query, params_hash
      end

      def exec_cast(query : String, types : Tuple(*TYPES)) forall TYPES
        exec_cast query, Map.new, types
      end

      # Execute the given query with the given parameters, returning an array
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      #
      # connection.exec_cast(<<-CYPHER, {email: "me@example.com"}, {User})
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      # ```
      def exec_cast(query : String, parameters : NamedTuple, types : Tuple)
        params = Neo4j::Map.new
        parameters.each do |key, value|
          params[key.to_s] = value.to_bolt_params
        end
        exec_cast query, params, types
      end

      # Execute the given query with the given parameters, yielding a tuple
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      #
      # connection.exec_cast(<<-CYPHER, {email: "me@example.com"}, {User}) do |(user)|
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      #   process user
      # end
      # ```
      def exec_cast(query : String, parameters : NamedTuple, types : Tuple, &block)
        params = Neo4j::Map.new
        parameters.each { |key, value| params[key.to_s] = value.to_bolt_params }
        exec_cast query, params, types do |row|
          yield row
        end
      end

      # Execute the given query with the given parameters, yielding a tuple
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      #
      # connection.exec_cast(<<-CYPHER, Neo4j::Map{"email" => "me@example.com"}, {User}) do |(user)|
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      #   process user
      # end
      # ```
      def exec_cast(query : String, parameters : Map, types : Tuple(*TYPES), metadata = Map.new, &block) : Nil forall TYPES
        send Commands::Run, query, parameters, metadata
        send Commands::Pull, Map{"n" => -1}

        query_result = read_result.as(Response)
        result = read_raw_result
        error = nil
        if query_result.is_a? Failure
          error = Exception.new
        end

        until result[1] != 0x71
          unless error
            # First 3 bytes are Structure, Record, and List
            # TODO: If the RETURN clause in the query has more than 16 items,
            # this will break because the List byte marker and its size won't be
            # in a single byte. We'll need to detect this here.

            io = IO::Memory.new(result + 3)
            begin
              yield types.from_bolt(io)
            rescue e
              error = e unless error
            end
          end

          result = read_raw_result
        end

        reset if error

        handle_result query_result
        # Don't try to parse the result unless we need to
        if result[1] == PackStream::Unpacker::StructureTypes::Failure.to_i
          reset
          handle_result PackStream::Unpacker.new(result).read.as(Response)
        end
      end

      def exec_cast(query : String, types : Tuple(*TYPES), &block) : Nil forall TYPES
        exec_cast query, Neo4j::Map.new, types do |row|
          yield row
        end
      end

      # Execute the given query with the given parameters, returning an array
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      #
      # connection.exec_cast(<<-CYPHER, Neo4j::Map{"email" => "me@example.com"}, {User})
      #   MATCH (user:User { email: $email })
      #   RETURN user
      # CYPHER
      # # => [{User(@id="4478440e-1897-41a9-812d-91f6d21b994b", @email="me@example.com")}]
      # ```
      def exec_cast(query : String, parameters : Map, types : Tuple(*TYPES)) forall TYPES
        {% begin %}
          results = Array(Tuple({{ TYPES.type_vars.map(&.instance).join(", ").id }})).new

          exec_cast query, parameters, types do |row|
            results << row
          end

          results
        {% end %}
      end

      # Execute the given query with the given parameters, returning an array
      # containing the results cast into the given types.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      #
      # connection.exec_cast_single(<<-CYPHER, Neo4j::Map{"email" => "me@example.com"}, {User})
      #   MATCH (user:User { email: $email })
      #   RETURN user
      #   LIMIT 1
      # CYPHER
      # # => {User(@id="4478440e-1897-41a9-812d-91f6d21b994b", @email="me@example.com")}
      # ```
      def exec_cast_single(query : String, parameters : Map, types : Tuple(*TYPES)) forall TYPES
        handled = false
        result = nil
        exec_cast query, parameters, types do |row|
          unless handled
            result = row
            handled = true
          end
        end

        result.not_nil!
      end

      # Execute the given query with the given parameters, returning a single
      # result cast to the given type.
      #
      # ```
      # struct User
      #   Neo4j.map_node(
      #     id: UUID,
      #     email: String,
      #   )
      # end
      #
      # connection.exec_cast_scalar(<<-CYPHER, Neo4j::Map{"email" => "me@example.com"}, User)
      #   MATCH (user:User { email: $email })
      #   RETURN user
      #   LIMIT 1
      # CYPHER
      # # => User(@id="4478440e-1897-41a9-812d-91f6d21b994b", @email="me@example.com")
      # ```
      def exec_cast_scalar(query : String, parameters : Map, type : T) forall T
        exec_cast_single(query, parameters, {type}).first
      end

      def exec_cast_scalar(query : String, type : T) forall T
        exec_cast_single(query, Neo4j::Map.new, {type}).first
      end

      def begin(metadata = Map.new)
        retry 5 do
          send Commands::Begin, metadata
          read_result
        end
      end

      def commit
        send Commands::Commit
        read_result
      end

      def rollback
        send Commands::Rollback
        read_result
      end

      # Wrap a group of queries into an atomic transaction. Yields a
      # `Neo4j::Bolt::Transaction`.
      #
      # ```
      # connection.transaction do |txn|
      #   connection.execute query1
      #   connection.execute query2
      # end
      # ```
      #
      # Exceptions raised within the block will roll back the transaction. To
      # roll back the transaction manually and exit the block, call
      # `txn.rollback`.
      def transaction(metadata = Map.new)
        if @transaction
          raise NestedTransactionError.new("Transaction already open, cannot open a new transaction")
        end

        transaction = @transaction = Transaction.new(self)

        self.begin(metadata)
        yield(transaction).tap { commit }
      rescue e : RollbackException
        rollback
      rescue e
        reset
        raise e
      ensure
        @transaction = nil
      end

      def close
        @connection.close
      end

      # If the connection gets into a wonky state, this method tells the server
      # to reset it back to a normal state, but you lose everything you haven't
      # pulled down yet.
      def reset
        send Commands::Reset
        read_result
      end

      private def init(username, password)
        send Commands::Hello, {
          "scheme"      => "basic",
          "principal"   => username,
          "credentials" => password,
          "user_agent"  => "Neo4j.cr/#{VERSION}",
        }
        @connection.flush

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

        total_slice = packer.to_slice
        length = total_slice.size

        message = IO::Memory.new.tap { |io|
          offset = 0
          while length - offset > 0
            slice = total_slice[offset, {(length - offset), 0xFFFF}.min]
            io.write_bytes(slice.size.to_u16, IO::ByteFormat::BigEndian)
            io.write slice
            offset += 0xFFFF
          end
          io.write_bytes 0x0000.to_u16, IO::ByteFormat::BigEndian
        }.to_slice
        @connection.write message
      end

      private def run(statement, parameters = Map.new, metadata = Map.new, retries = 5)
        send Commands::Run, statement, parameters, metadata

        result = read_result
        case result
        when Failure
          handle_result result
        when Success, Ignored
          result
        else
          raise ::Neo4j::UnknownResult.new("Cannot identify this result: #{result.inspect}")
        end
      rescue ex : IO::Error | OpenSSL::SSL::Error
        if retries > 0 && @transaction.nil?
          initialize @uri, @ssl
          run statement, parameters, metadata, retries - 1
        else
          raise ex
        end
      end

      private def send(command : Commands, *fields)
        write_message do |msg|
          msg.write_structure_start fields.size
          msg.write_byte command
          fields.each do |field|
            msg.write field
          end
        end
        @data_waiting = true
      end

      private def pull_all(&block) : Nil
        send Commands::Pull, Map{"n" => -1}

        result = read_result

        until result.is_a?(Response)
          yield result.as(List)
          result = read_result
        end

        handle_result result.as(Response)
      end

      private def pull_all : Array(List)
        results = Array(List).new
        pull_all { |result| results << result }

        results
      end

      private def read_result
        PackStream.unpack(read_raw_result)
      end

      EXCEPTIONS = {
        "Neo.ClientError.Schema.IndexAlreadyExists"                => IndexAlreadyExists,
        "Neo.ClientError.Schema.ConstraintValidationFailed"        => ConstraintValidationFailed,
        "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists" => EquivalentSchemaRuleAlreadyExists,
        "Neo.ClientError.Procedure.ProcedureCallFailed"            => ProcedureCallFailed,
        "Neo.ClientError.Statement.ParameterMissing"               => ParameterMissing,
        "Neo.ClientError.Statement.SyntaxError"                    => SyntaxError,
        "Neo.ClientError.Statement.ArgumentError"                  => ArgumentError,
      }

      private def handle_result(result : Failure)
        exception_class = EXCEPTIONS[result.attrs["code"].as(String)]? || QueryException
        raise exception_class.new(
          result.attrs["message"].as(String | Nil).to_s,
          result.attrs["code"].as(String),
        )
      end

      private def handle_result(result : Ignored)
      end

      private def handle_result(result : Success)
      end

      # Read a single result from the server in 64kb chunks. This is a bit of a
      # naive implementation that buffers the chunks until it has the full
      # result, then flattens out the chunks into a single slice and parses
      # that slice.
      private def read_raw_result
        @connection.flush if @data_waiting
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

        @data_waiting = false
        bytes
      end

      private def write(value)
        @connection.write_bytes value, IO::ByteFormat::BigEndian
        @data_waiting = true
      end

      private def write_value(value)
        @connection.write PackStream.pack(value)
        @data_waiting = true
      end

      private def handshake
        @connection.write GOGOBOLT
        SUPPORTED_VERSIONS.each do |ver|
          @connection.write_bytes ver, IO::ByteFormat::BigEndian
        end
        @connection.flush

        # Read server version
        # @todo Verify this
        @connection.read_bytes(
          Int32,
          IO::ByteFormat::BigEndian
        )
      end

      private def send_message(string : String)
        send_message string.to_slice
        @data_waiting = true
      end

      private def send_message(bytes : Bytes)
        @connection.write bytes
        @data_waiting = true
      end

      private def retry(times)
        loop do
          return yield
        rescue ex : IO::Error | OpenSSL::SSL::Error
          if times > 0
            initialize @uri, @ssl
            times -= 1
          else
            raise ex
          end
        end
      end
    end
  end
end
